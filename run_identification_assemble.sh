#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <n> [target_label]"
    echo "Example: $0 11 small"
    exit 1
fi

N=$1
LABEL=${2:-"small"}
WORKERS=${3:-2}

echo "Building JAR..."
sbt clean assembly
cp target/scala-2.12/graph-classifier.jar /srv/spark-jars/

MASTER=$(docker ps --filter "name=spark-master" --format "{{.ID}}")
if [ -z "$MASTER" ]; then
    echo "ERROR: Spark master container not found!"
    exit 1
fi

echo "Running identification for N=$N using target graph: $LABEL..."

# Optimized configuration for cluster:
# Linux worker:   6 cores, 6G memory
# Windows worker: 2 cores, 8G memory
# Total:          8 cores, 14G memory

docker exec $MASTER /opt/spark/bin/spark-submit \
  --master spark://192.168.0.107:7077 \
  --deploy-mode client \
  --class "main.IdentificationSparkProcess" \
  --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12,org.jgrapht:jgrapht-core:1.5.1 \
  --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2" \
  --driver-memory 2G \
  --executor-memory 1500M \
  /jars/graph-classifier.jar $N $LABEL $WORKERS

echo "Identification for N=$N with label '$LABEL' completed!"
