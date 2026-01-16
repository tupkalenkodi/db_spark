#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <n>"
    echo "Example: $0 12"
    exit 1
fi

N=$1
WORKERS=${2:-0}

MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")
if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    echo "Make sure your Docker Swarm cluster is running:"
    echo "  docker stack ps spark"
    exit 1
fi

echo "Running classification for n=$N..."

# Optimized configuration for cluster:
# Linux worker:   6 cores, 6G memory
# Windows worker: 2 cores, 8G memory
# Total:          8 cores, 14G memory

docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.ClassificationSparkProcess" \
  --driver-memory 2G \
  --executor-memory 1500M \
  --executor-cores 2 \
  --conf spark.default.parallelism=16 \
  /jars/graph-classifier.jar $N $WORKERS

echo "Classification for n=$N completed!"
