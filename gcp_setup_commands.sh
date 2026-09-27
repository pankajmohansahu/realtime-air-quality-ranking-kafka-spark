#!/bin/bash
# ============================================================
# OPPE 2 — Full GCP Setup Commands
# Run in Google Cloud Shell
# ============================================================

PROJECT_ID="project-399f087e-fef4-4831-b4e"
BUCKET="iitm-ibd-oppe2-21f2001203"
REGION="us-central1"
ZONE="us-central1-a"
KAFKA_VM="kafka-vm-oppe2"
CLUSTER_NAME="oppe2-cluster"

gcloud config set project $PROJECT_ID

# ── STEP 1: Upload dataset to GCS ──────────────────────────
gsutil cp producer.py          gs://${BUCKET}/scripts/producer.py
gsutil cp consumer.py          gs://${BUCKET}/scripts/consumer.py
echo "Files uploaded to GCS!"

# ── STEP 2: Create Kafka VM ─────────────────────────────────
gcloud compute instances create $KAFKA_VM \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=e2-standard-2 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --scopes=cloud-platform \
    --tags=kafka-server

# Open Kafka port 9092
gcloud compute firewall-rules create allow-kafka-9092 \
    --allow=tcp:9092 \
    --target-tags=kafka-server \
    --description="Allow Kafka broker traffic" \
    --quiet

echo "Kafka VM created: $KAFKA_VM"
echo "SSH into it: gcloud compute ssh $KAFKA_VM --zone=$ZONE"

# ── STEP 3: Create Dataproc Cluster ────────────────────────
# IMPORTANT: Install kafka-python on all nodes via pip-install init action
gcloud dataproc clusters create $CLUSTER_NAME \
    --project=$PROJECT_ID \
    --region=$REGION \
    --zone=${ZONE} \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100GB \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --image-version=2.1-debian11 \
    --metadata="PIP_PACKAGES=kafka-python" \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
    --properties="spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"

echo "Dataproc cluster created: $CLUSTER_NAME"

# ── STEP 4: Get Kafka VM Internal IP ───────────────────────
KAFKA_INTERNAL_IP=$(gcloud compute instances describe $KAFKA_VM \
    --zone=$ZONE \
    --format="get(networkInterfaces[0].networkIP)")
echo "Kafka VM Internal IP: $KAFKA_INTERNAL_IP"
echo "Use this as --kafka-broker: ${KAFKA_INTERNAL_IP}:9092"

# ── STEP 5: Submit Producer Job (PySpark batch) ─────────────
# Run AFTER Kafka is running and topic is created
gcloud dataproc jobs submit pyspark \
    gs://${BUCKET}/scripts/producer.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2" \
    -- \
    --kafka-broker ${KAFKA_INTERNAL_IP}:9092

# ── STEP 6: Submit Consumer Job (Spark Structured Streaming)
# Run this in a SEPARATE terminal BEFORE or alongside the producer
# For full demo (8-hour window):
gcloud dataproc jobs submit pyspark \
    gs://${BUCKET}/scripts/consumer.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2" \
    -- \
    --kafka-broker ${KAFKA_INTERNAL_IP}:9092 \
    --window-hours 8 \
    --slide-hours 1

# For DEMO with smaller window (faster output):
# -- --kafka-broker ${KAFKA_INTERNAL_IP}:9092 --window-hours 2 --slide-hours 1

# ── STEP 7: Cleanup (after exam) ────────────────────────────
# gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
# gcloud compute instances delete $KAFKA_VM --zone=$ZONE --quiet
