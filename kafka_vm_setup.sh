#!/bin/bash
# ============================================================
# OPPE 2 — Kafka VM Setup Script
# Run this INSIDE the Kafka VM after SSH
# ============================================================

# ── STEP 1: Install Java ────────────────────────────────────
sudo apt update -y
sudo apt install -y default-jdk wget netcat-openbsd
java -version

# ── STEP 2: Download & Install Kafka ───────────────────────
cd ~
wget https://downloads.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
tar -xvzf kafka_2.13-3.7.2.tgz
mv kafka_2.13-3.7.2 kafka
echo "Kafka installed!"

# ── STEP 3: Start Zookeeper (Terminal / tmux pane 1) ───────
# cd ~/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties

# ── STEP 4: Configure & Start Kafka (Terminal / tmux pane 2)
# Get this VM's external IP and update server.properties
KAFKA_EXTERNAL_IP=$(curl -s ifconfig.me)
echo "External IP: $KAFKA_EXTERNAL_IP"

cd ~/kafka
# Set listeners to bind on all interfaces
grep -q "^listeners=" config/server.properties \
    && sed -i 's|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' config/server.properties \
    || echo "listeners=PLAINTEXT://0.0.0.0:9092" >> config/server.properties

# Advertise external IP so Dataproc can reach Kafka
grep -q "^advertised.listeners=" config/server.properties \
    && sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092|" config/server.properties \
    || echo "advertised.listeners=PLAINTEXT://$KAFKA_EXTERNAL_IP:9092" >> config/server.properties

echo "Kafka server.properties updated with external IP: $KAFKA_EXTERNAL_IP"

# ── STEP 5: Create the Kafka Topic ─────────────────────────
# Run AFTER Zookeeper + Kafka are both running
# cd ~/kafka
# bin/kafka-topics.sh --create \
#     --topic air-quality-topic \
#     --bootstrap-server localhost:9092 \
#     --partitions 1 \
#     --replication-factor 1

# Verify topic
# bin/kafka-topics.sh --list --bootstrap-server localhost:9092
