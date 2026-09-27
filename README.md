# OPPE 2 — Real-Time Air Quality Streaming Pipeline
**Course:** Introduction to Big Data (Jan 2026 Term)  
**Student:** 21f2001203  
**Deadline:** 2026-04-26, 18:00 IST  

---

## Table of Contents
1. [Problem Statement](#1-problem-statement)
2. [Dataset](#2-dataset)
3. [Architecture Overview](#3-architecture-overview)
4. [Variable Definitions](#4-variable-definitions)
5. [GCP Infrastructure](#5-gcp-infrastructure)
6. [Component 1 — Kafka on Dedicated VM](#6-component-1--kafka-on-dedicated-vm)
7. [Component 2 — PySpark Producer on Dataproc](#7-component-2--pyspark-producer-on-dataproc)
8. [Component 3 — Spark Structured Streaming Consumer on Dataproc](#8-component-3--spark-structured-streaming-consumer-on-dataproc)
9. [Data Cleaning Strategy](#9-data-cleaning-strategy)
10. [Windowing Logic](#10-windowing-logic)
11. [Ranking Logic](#11-ranking-logic)
12. [Sample Output](#12-sample-output)
13. [Execution Steps](#13-execution-steps)
14. [Evaluation Coverage](#14-evaluation-coverage)

---

## 1. Problem Statement

This project is part of the **UN Sustainable Development Goals – Urban Health Index** initiative, which evaluates environmental livability across major global cities using real-time pollution indicators.

The task is to design and implement a **real-time streaming pipeline** that:
- Reads hourly air quality data for 50 cities across 15 days
- Publishes a **live ranking** of cities based on air quality stability and pollutant exposure
- Computes two variables over **8-hour sliding windows** (sliding every 1 hour):
  - **V1:** Average AQI over the last 8 hours (lower = better air quality)
  - **V2:** Maximum pollutant spike = max(1/pm25, 1/pm10, 1/no2, 1/so2, 1/o3, 1/co)
- Emits a rank-ordered list of all 50 cities for every valid window

### Ranking Rule
- **Primary:** Lower V1 = better rank (cleaner average air)
- **Tiebreaker:** Lower V2 = better rank

### Implementation Constraints
- **Producer:** PySpark batch job on Dataproc — reads from GCS, cleans data, sends to Kafka
- **Kafka:** Runs on a **separate dedicated GCE VM** (not on Dataproc)
- **Consumer:** Spark Structured Streaming job on Dataproc — reads from Kafka, computes windows, emits rankings

---

## 2. Dataset

**Source:** [Kaggle — Global Air Quality Data (15 days, hourly, 50 cities)](https://www.kaggle.com/datasets/smeet888/global-air-quality-data15-days-hourly-50-cities)

**File:** `globalAirQuality.csv`  
**Size:** 18,000 rows (50 cities × 360 hours)  
**Stored at:** `gs://iitm-ibd-oppe2-21f2001203/globalAirQuality.csv`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp | Hourly reading time |
| country | string | Country name |
| city | string | City name (50 unique cities) |
| latitude | double | Geographic latitude |
| longitude | double | Geographic longitude |
| pm25 | double | Fine particulate matter (µg/m³) |
| pm10 | double | Coarse particulate matter (µg/m³) |
| no2 | double | Nitrogen dioxide (µg/m³) |
| so2 | double | Sulfur dioxide (µg/m³) |
| o3 | double | Ozone (µg/m³) |
| co | double | Carbon monoxide (mg/m³) |
| aqi | integer | Air Quality Index |
| temperature | double | Temperature (°C) |
| humidity | double | Relative humidity (%) |
| wind_speed | double | Wind speed (m/s) |

**Key properties of the dataset:**
- Data spans 2025-11-04 to 2025-11-18 (15 days = 360 hours per city)
- Input is NOT globally sorted — it is grouped by city
- Data starts at 2025-11-04 18:25:17 UTC
- No null values in raw data, but pipeline handles nulls defensively

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        GCS BUCKET                               │
│          gs://iitm-ibd-oppe2-21f2001203/                        │
│  globalAirQuality.csv  │  scripts/producer.py  │  consumer.py   │
└────────────────────────┬────────────────────────────────────────┘
                         │ reads CSV
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATAPROC CLUSTER (oppe2-cluster)                   │
│                     us-central1-a                               │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PySpark Producer Job (Batch)                            │   │
│  │  - Reads globalAirQuality.csv from GCS                   │   │
│  │  - Cleans: drops nulls/bad rows (aqi ≤ 0)               │   │
│  │  - Sorts by timestamp ascending                          │   │
│  │  - Forward-fills missing hours per city                  │   │
│  │  - Sends 50 city records per hour → Kafka every 1 sec   │   │
│  └─────────────────────┬────────────────────────────────────┘   │
└────────────────────────┼────────────────────────────────────────┘
                         │ sends JSON records to port 9092
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│           KAFKA VM (kafka-vm-oppe2) — SEPARATE GCE VM           │
│           Internal IP: 10.128.0.23  │  Port: 9092              │
│                                                                 │
│   Zookeeper (port 2181)  +  Kafka Broker (port 9092)           │
│   Topic: air-quality-topic  │  Partitions: 1  │  Replication: 1│
└─────────────────────────────┬───────────────────────────────────┘
                              │ consumes from topic
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATAPROC CLUSTER (oppe2-cluster)                   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Spark Structured Streaming Consumer Job                 │   │
│  │  - Reads from air-quality-topic (startingOffsets=earliest│   │
│  │  - Parses JSON with defined schema                       │   │
│  │  - Applies watermark: (window_hours + 1) hours          │   │
│  │  - 8-hour sliding window (slides every 1 hour)          │   │
│  │  - V1 = avg(aqi) per city per window                    │   │
│  │  - V2 = max(1/pm25, 1/pm10, 1/no2, 1/so2, 1/o3, 1/co) │   │
│  │  - Ranks all 50 cities: lower V1 first, V2 tiebreaker   │   │
│  │  - Prints ranked table to console every 30 seconds      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Separation of Components (Required by Assignment)
| Component | Machine | Evidence |
|-----------|---------|---------|
| Producer | Dataproc cluster (oppe2-cluster) | Submitted via `gcloud dataproc jobs submit pyspark` |
| Kafka | kafka-vm-oppe2 (dedicated GCE VM) | Separate VM, Internal IP 10.128.0.23 |
| Consumer | Dataproc cluster (oppe2-cluster) | Submitted via `gcloud dataproc jobs submit pyspark` |

---

## 4. Variable Definitions

### V1 — Average AQI
```
V1 = avg(aqi) over 8-hour window per city
```
- Represents the **mean air pollution level** experienced in the city over the last 8 hours
- Lower V1 = cleaner air = **better rank**
- Computed using Spark's `F.avg("aqi")` grouped by city and window

### V2 — Maximum Pollutant Spike
```
V2 = max( 1/pm25, 1/pm10, 1/no2, 1/so2, 1/o3, 1/co ) over 8-hour window per city
```
- Represents the **inverse-concentration spike** across all pollutant types
- Used only as a **tiebreaker** when two cities have identical V1
- Lower V2 = better tiebreaker rank
- Computed using `F.max(F.greatest(F.lit(1.0)/F.col("pm25"), ..., F.lit(1.0)/F.col("co")))`

---

## 5. GCP Infrastructure

### Project & Bucket
| Resource | Value |
|----------|-------|
| Project ID | `project-399f087e-fef4-4831-b4e` |
| GCS Bucket | `iitm-ibd-oppe2-21f2001203` |
| Region | `us-central1` |
| Zone | `us-central1-a` |

### Kafka VM
| Property | Value |
|----------|-------|
| Name | `kafka-vm-oppe2` |
| Machine type | `e2-standard-2` (2 vCPU, 8GB RAM) |
| OS | Ubuntu 22.04 LTS |
| Internal IP | `10.128.0.23` |
| External IP | `34.56.39.5` |
| Kafka port | `9092` (firewall rule: `allow-kafka-9092`) |
| Kafka version | `2.13-3.7.1` |

### Dataproc Cluster
| Property | Value |
|----------|-------|
| Name | `oppe2-cluster` |
| Master | `n1-standard-4` (4 vCPU, 15GB RAM) |
| Workers | 2 × `n1-standard-4` |
| Image version | `2.1-debian11` (Spark 3.3) |
| Pip packages | `kafka-python` (via init action) |
| Spark packages | `spark-sql-kafka-0-10_2.12:3.3.2` |

---

## 6. Component 1 — Kafka on Dedicated VM

### Setup Steps
```bash
# Install Java and Kafka
sudo apt update -y && sudo apt install -y default-jdk wget netcat-openbsd
wget https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xvzf kafka_2.13-3.7.1.tgz && mv kafka_2.13-3.7.1 kafka

# Configure listeners (bind all interfaces)
grep -q "^listeners=" config/server.properties \
    && sed -i 's|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' config/server.properties \
    || echo "listeners=PLAINTEXT://0.0.0.0:9092" >> config/server.properties

# Advertise external IP so Dataproc can reach Kafka
KAFKA_EXTERNAL_IP=$(curl -s ifconfig.me)
grep -q "^advertised.listeners=" config/server.properties \
    && sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092|" config/server.properties \
    || echo "advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092" >> config/server.properties

# Start Zookeeper (Terminal 1)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker (Terminal 2)
bin/kafka-server-start.sh config/server.properties

# Create topic (Terminal 3)
bin/kafka-topics.sh --create \
    --topic air-quality-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

### Topic Configuration
| Property | Value | Reason |
|----------|-------|--------|
| Topic name | `air-quality-topic` | Matches producer and consumer |
| Partitions | 1 | Single stream, ordered delivery |
| Replication factor | 1 | Single-node Kafka VM |

---

## 7. Component 2 — PySpark Producer on Dataproc

**File:** `producer.py`  
**Submitted via:** `gcloud dataproc jobs submit pyspark gs://.../scripts/producer.py`

### What It Does
1. Reads `globalAirQuality.csv` from GCS using PySpark
2. Applies full data cleaning pipeline (see Section 9)
3. Groups cleaned records by timestamp (50 cities per hour)
4. Sends each group to Kafka topic `air-quality-topic` as JSON
5. Waits 1 second between groups (simulates real-time hourly stream)

### Key Code — Kafka Send Loop
```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

by_timestamp = defaultdict(list)
for r in records:
    by_timestamp[r["timestamp"]].append(r)

for ts in sorted(by_timestamp.keys()):
    batch = by_timestamp[ts]
    for record in batch:
        producer.send(KAFKA_TOPIC, record)
    producer.flush()
    time.sleep(1)  # 1 second per hour of data
```

### Producer Output
```
[Producer] Raw rows loaded: 18000
[Producer] Rows after cleaning bad rows: 18000
[Producer] Expected hours per city: 360
[Producer] Rows after gap-filling: 18000
[Producer] Connecting to Kafka broker: 10.128.0.23:9092
[Producer] Total records to send: 18000
[Producer] Hour   1 | Timestamp: 2025-11-04 18:25:17 | Sent 50 city records | Total: 50/18000
[Producer] Hour   2 | Timestamp: 2025-11-04 19:25:17 | Sent 50 city records | Total: 100/18000
...
[Producer] Hour 360 | Timestamp: 2025-11-18 17:25:17 | Sent 50 city records | Total: 18000/18000
```

---

## 8. Component 3 — Spark Structured Streaming Consumer on Dataproc

**File:** `consumer.py`  
**Submitted via:** `gcloud dataproc jobs submit pyspark gs://.../scripts/consumer.py`

### What It Does
1. Reads streaming records from Kafka topic `air-quality-topic`
2. Parses JSON payload using a defined schema
3. Applies event-time watermark for late data handling
4. Groups by city and sliding time window
5. Computes V1 (avg AQI) and V2 (max pollutant spike) per group
6. Uses `foreachBatch` to rank cities and print formatted table every 30 seconds

### Key Code — Streaming Read + Window
```python
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "air-quality-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(F.from_json(F.col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withWatermark("timestamp", f"{WINDOW_HOURS + 1} hours")

window_spec = F.window("timestamp", f"{WINDOW_HOURS} hours", f"{SLIDE_HOURS} hour")

city_window_stats = df.groupBy("city", window_spec).agg(
    F.round(F.avg("aqi"), 4).alias("V1_avg_aqi"),
    F.round(F.max(F.greatest(
        F.lit(1.0)/F.col("pm25"), F.lit(1.0)/F.col("pm10"),
        F.lit(1.0)/F.col("no2"),  F.lit(1.0)/F.col("so2"),
        F.lit(1.0)/F.col("o3"),   F.lit(1.0)/F.col("co")
    )), 4).alias("V2_max_pollutant_spike"),
    F.count("*").alias("record_count")
)
```

### Command Line Arguments
| Argument | Default | Description |
|----------|---------|-------------|
| `--kafka-broker` | required | Kafka broker address e.g. `10.128.0.23:9092` |
| `--window-hours` | 8 | Window size in hours (use 2 for demo) |
| `--slide-hours` | 1 | Slide interval in hours |

### Checkpoint Location
`gs://iitm-ibd-oppe2-21f2001203/checkpoints/consumer`

---

## 9. Data Cleaning Strategy

The producer handles three data quality issues mandated by the problem statement:

### Issue 1 — Bad Rows
**Detection:** Rows with null values in any key column, or AQI ≤ 0  
**Fix:** Filter them out before processing
```python
df = df.filter(
    F.col("timestamp").isNotNull() &
    F.col("city").isNotNull() &
    F.col("aqi").isNotNull() &
    (F.col("aqi") > 0) &
    F.col("pm25").isNotNull() & F.col("pm10").isNotNull() &
    F.col("no2").isNotNull()  & F.col("so2").isNotNull() &
    F.col("o3").isNotNull()   & F.col("co").isNotNull()
)
```

### Issue 2 — Unsorted Input
**Detection:** Data is grouped by city, not sorted globally by time  
**Fix:** Sort by timestamp and city before processing
```python
df = df.orderBy("timestamp", "city")
```

### Issue 3 — Missing Hours per City
**Detection:** Some cities may not have readings for every hour  
**Fix:** Create a full timestamp spine, cross-join with all cities, left-join actual data, then forward-fill nulls using window function
```python
# Create complete hourly timestamp spine
timestamps_df = spark.createDataFrame(
    [(min_ts + timedelta(hours=i),) for i in range(hours_count)], ["timestamp"]
)

# Cross join with all 50 cities
full_grid = timestamps_df.crossJoin(cities_df)

# Left join actual data
df_filled = full_grid.join(df, on=["timestamp", "city"], how="left")

# Forward fill using last() with ignorenulls
window_ffill = Window.partitionBy("city").orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

for col_name in ["pm25", "pm10", "no2", "so2", "o3", "co", "aqi", ...]:
    df_filled = df_filled.withColumn(
        col_name,
        F.last(F.col(col_name), ignorenulls=True).over(window_ffill)
    )
```

---

## 10. Windowing Logic

### Sliding Window Parameters
| Parameter | Full Run | Demo Run |
|-----------|----------|----------|
| Window size | 8 hours | 2 hours |
| Slide interval | 1 hour | 1 hour |
| Watermark delay | 9 hours | 3 hours |

### How Many Windows
With 360 hours of data and a 2-hour window sliding every 1 hour:
- Total windows = 360 - 2 + 1 = **359 windows**
- Each window covers 50 cities
- Total ranking outputs = 359 × 50 = **17,950 rows**

### Watermark
```python
.withWatermark("timestamp", f"{WINDOW_HOURS + 1} hours")
```
The watermark is set to `WINDOW_HOURS + 1` hours to allow for late-arriving data while still bounding state size.

### Event-Time vs Processing-Time
The pipeline uses **event-time windowing** — windows are based on the actual timestamps in the data, not the wall-clock time when records arrive. This is critical for correct air quality analysis since records represent specific historical hours.

---

## 11. Ranking Logic

### foreachBatch Pattern
```python
def rank_and_print(batch_df, epoch_id):
    batch_df = batch_df.withColumn("window_end", F.col("window.end"))
    window_ends = [r["window_end"] for r in batch_df.select("window_end").distinct().collect()]

    for wend in sorted(window_ends):
        window_data = batch_df.filter(F.col("window_end") == wend)
        ranked = window_data.orderBy(
            F.col("V1_avg_aqi").asc(),
            F.col("V2_max_pollutant_spike").asc()
        )
        # Print formatted table...
```

### Why foreachBatch?
- `outputMode("update")` with windowed aggregations requires `foreachBatch`
- Allows custom sorting and formatted printing within each micro-batch
- Handles multiple windows that arrive in the same batch

---

## 12. Sample Output

```
================================================================================
  EPOCH: 1  |  WINDOW: 2025-11-04 18:00:00  -->  2025-11-04 20:00:00
  Cities in window: 50
================================================================================
  RANK   CITY                   V1 (Avg AQI)     V2 (Max Spike)   RECORDS
--------------------------------------------------------------------------------
  1      Singapore              76.0000          0.9843           2
  2      Mumbai                 76.0000          2.3148           2
  3      Nairobi                78.0000          4.2918           2
  4      Jakarta                78.5000          4.7393           2
  5      Seoul                  80.5000          2.5773           2
  ...
  50     Karachi                173.0000         1.2706           2
================================================================================

================================================================================
  EPOCH: 1  |  WINDOW: 2025-11-04 19:00:00  -->  2025-11-04 21:00:00
  Cities in window: 50
================================================================================
  RANK   CITY                   V1 (Avg AQI)     V2 (Max Spike)   RECORDS
--------------------------------------------------------------------------------
  1      Toronto                66.5000          1.1547           2
  2      Cape Town              67.0000          1.3680           2
  3      Berlin                 68.0000          1.0493           2
  ...
================================================================================
```

### What the Rankings Show
- **Singapore ranks #1** in window 18:00→20:00 with avg AQI of 76 — cleanest air in that period
- **Toronto jumps to #1** in window 19:00→21:00 with avg AQI of 66.5 — shows dynamic nature of rankings
- Rankings change every hour as the sliding window drops old data and adds new data
- All 50 cities always appear (guaranteed by producer's gap-filling)
- RECORDS=2 means 2 hourly readings per city per 2-hour window ✅

---

## 13. Execution Steps

### Prerequisites
- GCS bucket created: `iitm-ibd-oppe2-21f2001203`
- `globalAirQuality.csv` uploaded to bucket root
- Both scripts uploaded: `gs://.../scripts/producer.py` and `consumer.py`
- Dataproc cluster `oppe2-cluster` running
- Kafka VM `kafka-vm-oppe2` running

### Step 1 — Start Zookeeper (SSH Tab 1 into kafka-vm-oppe2)
```bash
cd ~/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties
# Wait for: "binding to port 0.0.0.0/0.0.0.0:2181"
```

### Step 2 — Start Kafka Broker (SSH Tab 2 into kafka-vm-oppe2)
```bash
cd ~/kafka && bin/kafka-server-start.sh config/server.properties
# Wait for: "[KafkaServer id=0] started"
```

### Step 3 — Create Topic (SSH Tab 3 into kafka-vm-oppe2)
```bash
cd ~/kafka
bin/kafka-topics.sh --create --topic air-quality-topic \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Step 4 — Start Consumer FIRST (Cloud Shell Tab 1)
```bash
gcloud dataproc jobs submit pyspark \
    gs://iitm-ibd-oppe2-21f2001203/scripts/consumer.py \
    --cluster=oppe2-cluster --region=us-central1 \
    --project=project-399f087e-fef4-4831-b4e \
    --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2" \
    -- --kafka-broker 10.128.0.23:9092 --window-hours 2 --slide-hours 1
# Wait for: "[Consumer] Streaming query started. Waiting for data..."
```

### Step 5 — Start Producer (Cloud Shell Tab 2)
```bash
gcloud dataproc jobs submit pyspark \
    gs://iitm-ibd-oppe2-21f2001203/scripts/producer.py \
    --cluster=oppe2-cluster --region=us-central1 \
    --project=project-399f087e-fef4-4831-b4e \
    -- --kafka-broker 10.128.0.23:9092
```

### Step 6 — View Ranked Output
After producer starts sending data, the consumer tab will begin printing ranked city tables. Scroll down or press `End` to see the latest output.

---

## 14. Evaluation Coverage

| Rubric Item | Marks | Implementation |
|-------------|-------|----------------|
| Live Demo — end-to-end pipeline across separate machines | 40 | Producer on Dataproc → Kafka VM → Consumer on Dataproc |
| Consumer (Streaming Job) | 20 | Spark Structured Streaming with 8-hour sliding window, V1/V2, ranking |
| Producer (Batch Job) | 15 | PySpark reads GCS, cleans data, sends to Kafka |
| Kafka Setup | 15 | Dedicated VM, Zookeeper + Broker + Topic creation |
| Explanation | 10 | Data flow, V1/V2 meaning, ranking logic documented here |
| **Total** | **100** | |

### Multi-Machine Separation Evidence
The problem statement warns: *"If the entire pipeline is executed on a single machine without proper separation of components, the maximum marks will be capped at 40/100."*

Our pipeline uses **3 separate machines**:
1. `oppe2-cluster-m` (Dataproc master) + `oppe2-cluster-w-0/1` (workers) → runs Producer and Consumer
2. `kafka-vm-oppe2` (dedicated GCE VM) → runs Kafka broker only

This is verifiable from:
- Dataproc Jobs page showing 2 PySpark jobs on `oppe2-cluster`
- Compute Engine VM instances page showing 4 VMs (1 Kafka + 3 Dataproc)
- Consumer connecting to `10.128.0.23:9092` (Kafka VM internal IP)

---

## Files in This Solution

| File | Purpose |
|------|---------|
| `producer.py` | PySpark batch job — reads GCS, cleans data, sends to Kafka |
| `consumer.py` | Spark Structured Streaming job — reads Kafka, computes windows, ranks cities |
| `kafka_vm_setup.sh` | Reference: Kafka VM setup commands |
| `gcp_setup_commands.sh` | Reference: All GCP resource creation commands |
| `COMMANDS_COPY_PASTE.txt` | Step-by-step copy-paste guide for running the pipeline |
| `STEP_BY_STEP_GUIDE.md` | Execution guide with terminal layout |
| `README.md` | This file — full project documentation |
