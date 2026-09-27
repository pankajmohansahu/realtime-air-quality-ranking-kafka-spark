"""
OPPE 2 - Air Quality Streaming Pipeline
Spark Structured Streaming Consumer — runs on Dataproc
Reads from Kafka, computes 8-hour sliding windows, ranks 50 cities every hour.

V1 = Average AQI over 8-hour window (lower = better rank)
V2 = Max pollutant spike over 8-hour window = max(pm25, pm10, no2, so2, o3, co)
     (tiebreaker: lower V2 = better rank)
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                                DoubleType, IntegerType, TimestampType)

# ─── ARGUMENT PARSING ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--kafka-broker", required=True,
                    help="Kafka broker address e.g. 10.128.0.X:9092")
parser.add_argument("--window-hours", type=int, default=8,
                    help="Window size in hours (default 8, reduce for demo)")
parser.add_argument("--slide-hours",  type=int, default=1,
                    help="Slide interval in hours (default 1)")
args, _ = parser.parse_known_args()

KAFKA_BROKER  = args.kafka_broker
KAFKA_TOPIC   = "air-quality-topic"
WINDOW_HOURS  = args.window_hours
SLIDE_HOURS   = args.slide_hours
BUCKET        = "iitm-ibd-oppe2-21f2001203"
CHECKPOINT    = f"gs://{BUCKET}/checkpoints/consumer"

print(f"[Consumer] Kafka broker  : {KAFKA_BROKER}")
print(f"[Consumer] Window        : {WINDOW_HOURS}h sliding every {SLIDE_HOURS}h")

# ─── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AirQuality_StreamingConsumer") \
    .config("spark.sql.streaming.statefulOperator.allowMultiple", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ─── KAFKA SCHEMA ──────────────────────────────────────────────────────────────
schema = StructType([
    StructField("timestamp",   StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("latitude",    DoubleType(),  True),
    StructField("longitude",   DoubleType(),  True),
    StructField("pm25",        DoubleType(),  True),
    StructField("pm10",        DoubleType(),  True),
    StructField("no2",         DoubleType(),  True),
    StructField("so2",         DoubleType(),  True),
    StructField("o3",          DoubleType(),  True),
    StructField("co",          DoubleType(),  True),
    StructField("aqi",         IntegerType(), True),
    StructField("temperature", DoubleType(),  True),
    StructField("humidity",    DoubleType(),  True),
    StructField("wind_speed",  DoubleType(),  True),
])

# ─── READ FROM KAFKA ───────────────────────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON value
df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(F.from_json(F.col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .filter(F.col("timestamp").isNotNull() & F.col("city").isNotNull() & F.col("aqi").isNotNull()) \
    .withWatermark("timestamp", f"{WINDOW_HOURS + 1} hours")

# ─── COMPUTE V1 AND V2 PER CITY PER WINDOW ─────────────────────────────────────
# V1 = avg(aqi) over 8-hour window per city
# V2 = max(pm25, pm10, no2, so2, o3, co) over 8-hour window per city

window_spec = F.window("timestamp",
                        f"{WINDOW_HOURS} hours",
                        f"{SLIDE_HOURS} hour")

city_window_stats = df \
    .groupBy("city", window_spec) \
    .agg(
        F.round(F.avg("aqi"),    4).alias("V1_avg_aqi"),
        F.round(F.max(
            F.greatest(
                F.lit(1.0) / F.col("pm25"),
                F.lit(1.0) / F.col("pm10"),
                F.lit(1.0) / F.col("no2"),
                F.lit(1.0) / F.col("so2"),
                F.lit(1.0) / F.col("o3"),
                F.lit(1.0) / F.col("co")
            )
        ), 4).alias("V2_max_pollutant_spike"),
        F.count("*").alias("record_count")
    )

# ─── RANKING via foreachBatch ──────────────────────────────────────────────────
def rank_and_print(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    # Add window_end as the "hour" identifier
    batch_df = batch_df.withColumn("window_end", F.col("window.end"))

    # Get all unique window_ends in this micro-batch
    window_ends = [r["window_end"] for r in batch_df.select("window_end").distinct().collect()]

    for wend in sorted(window_ends):
        window_data = batch_df.filter(F.col("window_end") == wend)
        count = window_data.count()
        if count < 1:
            continue

        # Sort: V1 ascending, then V2 ascending (lower = better)
        ranked = window_data \
            .orderBy(F.col("V1_avg_aqi").asc(), F.col("V2_max_pollutant_spike").asc()) \
            .select("city", "V1_avg_aqi", "V2_max_pollutant_spike", "record_count")

        wstart = wend - __import__('datetime').timedelta(hours=WINDOW_HOURS)

        print("\n" + "=" * 80)
        print(f"  EPOCH: {epoch_id}  |  WINDOW: {wstart}  -->  {wend}")
        print(f"  Cities in window: {count}")
        print("=" * 80)
        print(f"  {'RANK':<6} {'CITY':<22} {'V1 (Avg AQI)':<16} {'V2 (Max Spike)':<16} {'RECORDS'}")
        print("-" * 80)

        rows = ranked.collect()
        for rank, row in enumerate(rows, start=1):
            print(f"  {rank:<6} {row['city']:<22} {row['V1_avg_aqi']:<16.4f} "
                  f"{row['V2_max_pollutant_spike']:<16.4f} {row['record_count']}")

        print("=" * 80)

# ─── START STREAMING QUERY ────────────────────────────────────────────────────
query = city_window_stats \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(rank_and_print) \
    .option("checkpointLocation", CHECKPOINT) \
    .trigger(processingTime="30 seconds") \
    .start()

print("[Consumer] Streaming query started. Waiting for data...")
query.awaitTermination()
