# OPPE 2 — Step-by-Step Execution Guide
**Project:** project-399f087e-fef4-4831-b4e | **Bucket:** iitm-ibd-oppe2-21f2001203

---

## Architecture
```
GCS (globalAirQuality.csv)
        │
        ▼
  Dataproc Cluster (Producer — PySpark batch job)
  - Reads CSV from GCS
  - Cleans: drop bad rows, forward-fill missing hours, sort by timestamp
  - Sends 1 hour's worth of records (50 cities) to Kafka every 1 second
        │
        ▼
  Kafka VM (kafka-vm-oppe2) ← SEPARATE GCE VM
  - Topic: air-quality-topic
  - Port: 9092
        │
        ▼
  Dataproc Cluster (Consumer — Spark Structured Streaming job)
  - 8-hour sliding window (slides every 1 hour)
  - V1 = avg(aqi) per city per window
  - V2 = max(pm25, pm10, no2, so2, o3, co) per city per window
  - Ranks all 50 cities: lower V1 = better; tiebreak = lower V2
  - Prints ranked table to console every 30 seconds
```

---

## TERMINAL LAYOUT (3 terminals needed)

| Terminal | What runs | Where |
|----------|-----------|-------|
| **T1** | Kafka VM — Zookeeper | SSH into kafka-vm-oppe2 |
| **T2** | Kafka VM — Kafka broker | SSH into kafka-vm-oppe2 |
| **T3** | Cloud Shell — Consumer job | Cloud Shell |
| **T4** | Cloud Shell — Producer job | Cloud Shell (new tab) |

---

## STEP 1 — Upload files to GCS (Cloud Shell)

```bash
PROJECT_ID="project-399f087e-fef4-4831-b4e"
BUCKET="iitm-ibd-oppe2-21f2001203"

# Upload dataset and scripts
gsutil cp globalAirQuality.csv gs://${BUCKET}/input/globalAirQuality.csv
gsutil cp producer.py          gs://${BUCKET}/scripts/producer.py
gsutil cp consumer.py          gs://${BUCKET}/scripts/consumer.py

# Verify
gsutil ls gs://${BUCKET}/input/
gsutil ls gs://${BUCKET}/scripts/
```

---

## STEP 2 — Create Kafka VM (Cloud Shell)

```bash
gcloud compute instances create kafka-vm-oppe2 \
    --project=project-399f087e-fef4-4831-b4e \
    --zone=us-central1-a \
    --machine-type=e2-standard-2 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --tags=kafka-server

gcloud compute firewall-rules create allow-kafka-9092 \
    --allow=tcp:9092 \
    --target-tags=kafka-server \
    --quiet
```

---

## STEP 3 — Create Dataproc Cluster (Cloud Shell)

```bash
gcloud dataproc clusters create oppe2-cluster \
    --project=project-399f087e-fef4-4831-b4e \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100GB \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --image-version=2.1-debian11 \
    --metadata="PIP_PACKAGES=kafka-python" \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
    --properties="spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"
```

---

## STEP 4 — Setup Kafka on Kafka VM (T1 + T2)

```bash
# SSH into Kafka VM
gcloud compute ssh kafka-vm-oppe2 --zone=us-central1-a
```

**Inside the VM (Terminal 1 — Zookeeper):**
```bash
sudo apt update -y && sudo apt install -y default-jdk wget netcat-openbsd
wget https://downloads.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
tar -xvzf kafka_2.13-3.7.2.tgz && mv kafka_2.13-3.7.2 kafka

# Start Zookeeper
cd ~/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Inside the VM (Terminal 2 — Kafka broker):**
```bash
cd ~/kafka

# Auto-detect and set external IP
KAFKA_EXTERNAL_IP=$(curl -s ifconfig.me)
grep -q "^listeners=" config/server.properties \
    && sed -i 's|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' config/server.properties \
    || echo "listeners=PLAINTEXT://0.0.0.0:9092" >> config/server.properties
grep -q "^advertised.listeners=" config/server.properties \
    && sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092|" config/server.properties \
    || echo "advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092" >> config/server.properties

bin/kafka-server-start.sh config/server.properties
```

**Inside the VM (Terminal 3 — Create topic):**
```bash
cd ~/kafka
bin/kafka-topics.sh --create \
    --topic air-quality-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

# Verify
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## STEP 5 — Get Kafka VM Internal IP (Cloud Shell)

```bash
KAFKA_IP=$(gcloud compute instances describe kafka-vm-oppe2 \
    --zone=us-central1-a \
    --format="get(networkInterfaces[0].networkIP)")
echo "Kafka Internal IP: $KAFKA_IP"
# Use this as: --kafka-broker ${KAFKA_IP}:9092
```

---

## STEP 6 — Start Consumer FIRST (T3 — Cloud Shell tab 1)

```bash
KAFKA_IP=$(gcloud compute instances describe kafka-vm-oppe2 \
    --zone=us-central1-a \
    --format="get(networkInterfaces[0].networkIP)")

# For demo (2-hour window for faster output):
gcloud dataproc jobs submit pyspark \
    gs://iitm-ibd-oppe2-21f2001203/scripts/consumer.py \
    --cluster=oppe2-cluster \
    --region=us-central1 \
    --project=project-399f087e-fef4-4831-b4e \
    --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2" \
    -- \
    --kafka-broker ${KAFKA_IP}:9092 \
    --window-hours 2 \
    --slide-hours 1

# For full 8-hour window (as per problem statement):
# -- --kafka-broker ${KAFKA_IP}:9092 --window-hours 8 --slide-hours 1
```

---

## STEP 7 — Start Producer (T4 — Cloud Shell tab 2)

```bash
KAFKA_IP=$(gcloud compute instances describe kafka-vm-oppe2 \
    --zone=us-central1-a \
    --format="get(networkInterfaces[0].networkIP)")

gcloud dataproc jobs submit pyspark \
    gs://iitm-ibd-oppe2-21f2001203/scripts/producer.py \
    --cluster=oppe2-cluster \
    --region=us-central1 \
    --project=project-399f087e-fef4-4831-b4e \
    -- \
    --kafka-broker ${KAFKA_IP}:9092
```

---

## Expected Consumer Output

```
================================================================================
  EPOCH: 5  |  WINDOW: 2025-11-04 18:00:00  -->  2025-11-05 02:00:00
  Cities in window: 50
================================================================================
  RANK   CITY                   V1 (Avg AQI)     V2 (Max Spike)   RECORDS
--------------------------------------------------------------------------------
  1      Zurich                 71.2500          45.3200          8
  2      Helsinki               73.1250          52.1100          8
  3      Stockholm              75.6250          48.9900          8
  ...
  50     Delhi                  158.3750         145.2200         8
================================================================================
```

---

## V1 and V2 Definitions

| Variable | Definition | Formula |
|----------|-----------|---------|
| **V1** | Average AQI over 8-hour window per city | `avg(aqi)` |
| **V2** | Max pollutant spike over 8-hour window | `max(greatest(pm25, pm10, no2, so2, o3, co))` |

**Ranking rule:** Lower V1 = better rank. If V1 tie → lower V2 = better rank.

---

## Data Cleaning Summary

| Issue | How Handled |
|-------|-------------|
| Bad rows (nulls, aqi ≤ 0) | `filter(aqi IS NOT NULL AND aqi > 0 AND ...)` |
| Unsorted input | `orderBy("timestamp", "city")` |
| Missing hours per city | Forward-fill using `last(col, ignorenulls=True)` over window |

---

## Fallback Zones (if us-central1-a fails)
```bash
--zone=us-central1-b
--zone=us-central1-f
--zone=us-east1-d
```
