#!/bin/bash

# Usage: ./run_classification.sh <n>
# Example: ./run_classification.sh 12

# Check if n is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <n>"
    echo "Example: $0 12"
    exit 1
fi

N=$1


# Set master container name
MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")

if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    echo "Make sure your Docker Swarm cluster is running:"
    echo "  docker stack ps spark"
    exit 1
fi

echo "Running classification for n=$N..."


# MINIMAL CONFIGURATION FOR OPTIMAL CORE USAGE
docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.ClassificationSparkProcess" \
  --driver-memory 2G \
  --executor-memory 800M \
  --executor-cores 1 \
  --conf spark.default.parallelism=16 \
  /jars/graph-classifier.jar $N

echo "Classification for n=$N completed!"


