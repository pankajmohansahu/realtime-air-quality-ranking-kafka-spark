"""
OPPE 2 - Air Quality Streaming Pipeline
PySpark Batch Producer — runs on Dataproc
Reads globalAirQuality.csv from GCS, cleans data, sends to Kafka topic.
"""

import argparse
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window
from kafka import KafkaProducer

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BUCKET          = "iitm-ibd-oppe2-21f2001203"
INPUT_PATH      = f"gs://{BUCKET}/globalAirQuality.csv"
KAFKA_TOPIC     = "air-quality-topic"
BATCH_DELAY_SEC = 1      # seconds between batches (1 record = 1 hour per city)
CITIES          = 50

# ─── ARGUMENT PARSING ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--kafka-broker", required=True,
                    help="Kafka broker address e.g. 10.128.0.X:9092")
args, _ = parser.parse_known_args()
KAFKA_BROKER = args.kafka_broker

# ─── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AirQuality_Producer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"[Producer] Reading data from: {INPUT_PATH}")

# ─── LOAD DATA ─────────────────────────────────────────────────────────────────
df_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(INPUT_PATH)

print(f"[Producer] Raw rows loaded: {df_raw.count()}")
df_raw.printSchema()

# ─── DATA CLEANING ─────────────────────────────────────────────────────────────

# 1. Cast timestamp properly
df = df_raw.withColumn("timestamp", F.to_timestamp("timestamp"))

# 2. Drop bad rows: null timestamp, null city, null aqi, non-positive pollutants
df = df.filter(
    F.col("timestamp").isNotNull() &
    F.col("city").isNotNull() &
    F.col("aqi").isNotNull() &
    (F.col("aqi") > 0) &
    F.col("pm25").isNotNull() &
    F.col("pm10").isNotNull() &
    F.col("no2").isNotNull() &
    F.col("so2").isNotNull() &
    F.col("o3").isNotNull() &
    F.col("co").isNotNull()
)

print(f"[Producer] Rows after cleaning bad rows: {df.count()}")

# 3. Sort by timestamp ascending (data is NOT pre-sorted globally)
df = df.orderBy("timestamp", "city")

# ─── HANDLE MISSING HOURS PER CITY ─────────────────────────────────────────────
# Get all unique (city, country, latitude, longitude) combos
cities_df = df.select("city", "country", "latitude", "longitude").distinct()

# Get min and max timestamps
time_bounds = df.agg(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
).collect()[0]

min_ts = time_bounds["min_ts"]
max_ts = time_bounds["max_ts"]

# Generate complete hourly timestamp spine
hours_count = int((max_ts - min_ts).total_seconds() // 3600) + 1
print(f"[Producer] Expected hours per city: {hours_count}")

# Create spine of all expected timestamps (hourly)
timestamps_rdd = spark.sparkContext.parallelize(
    [(min_ts + __import__('datetime').timedelta(hours=i),) for i in range(hours_count)]
)
timestamps_df = spark.createDataFrame(timestamps_rdd, ["timestamp"])

# Cross join timestamps with all cities to create full grid
full_grid = timestamps_df.crossJoin(cities_df)

# Left join actual data onto full grid
df_filled = full_grid.join(
    df.select("timestamp", "city", "pm25", "pm10", "no2", "so2", "o3", "co", "aqi",
              "temperature", "humidity", "wind_speed"),
    on=["timestamp", "city"],
    how="left"
)

# Forward fill missing values per city ordered by timestamp
# Use last() with ignorenulls over an unbounded window per city
window_ffill = Window.partitionBy("city").orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

for col_name in ["pm25", "pm10", "no2", "so2", "o3", "co", "aqi",
                 "temperature", "humidity", "wind_speed"]:
    df_filled = df_filled.withColumn(
        col_name,
        F.last(F.col(col_name), ignorenulls=True).over(window_ffill)
    )

# Drop rows still null after ffill (rows before first data point for a city)
df_filled = df_filled.filter(F.col("aqi").isNotNull())

# Final sort
df_filled = df_filled.orderBy("timestamp", "city")

print(f"[Producer] Rows after gap-filling: {df_filled.count()}")

# ─── SEND TO KAFKA ─────────────────────────────────────────────────────────────
print(f"[Producer] Connecting to Kafka broker: {KAFKA_BROKER}")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Collect and send row by row, grouped by timestamp (simulate hourly stream)
rows = df_filled.collect()
total = len(rows)
print(f"[Producer] Total records to send: {total}")

# Group by timestamp — send all 50 cities for each hour together
from itertools import groupby
from operator import itemgetter

# Convert to list of dicts
records = [row.asDict() for row in rows]

# Convert timestamps to string for JSON serialization
for r in records:
    r["timestamp"] = str(r["timestamp"])

sent = 0
hour_num = 0
# Group records by timestamp
from collections import defaultdict
by_timestamp = defaultdict(list)
for r in records:
    by_timestamp[r["timestamp"]].append(r)

for ts in sorted(by_timestamp.keys()):
    batch = by_timestamp[ts]
    for record in batch:
        producer.send(KAFKA_TOPIC, record)
    producer.flush()
    sent += len(batch)
    hour_num += 1
    print(f"[Producer] Hour {hour_num:>3} | Timestamp: {ts} | Sent {len(batch):>2} city records | Total: {sent}/{total}")
    time.sleep(BATCH_DELAY_SEC)

print(f"[Producer] Done. Total records sent: {sent}")
spark.stop()
