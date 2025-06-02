#!/bin/bash

echo "Starting Spark Streaming Job in Docker..."

# Get the current directory
CURRENT_DIR=$(pwd)
echo "Current directory: $CURRENT_DIR"

# Check if docker-compose.yml exists in different locations
if [ -f "./docker/docker-compose.yml" ]; then
    COMPOSE_FILE="./docker/docker-compose.yml"
elif [ -f "../docker/docker-compose.yml" ]; then
    COMPOSE_FILE="../docker/docker-compose.yml"
elif [ -f "./docker-compose.yml" ]; then
    COMPOSE_FILE="./docker-compose.yml"
else
    echo "Error: docker-compose.yml not found!"
    echo "Searched in: ./docker/, ../docker/, and current directory"
    exit 1
fi

echo "Using docker-compose file: $COMPOSE_FILE"

# Check if Docker Compose services are running
if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
    echo "Docker services are not running. Starting infrastructure first..."
    docker-compose -f "$COMPOSE_FILE" up -d
    echo "Waiting for services to be ready..."
    sleep 30
fi

# Check if spark container exists (could be spark-master, docker-spark-1, etc.)
SPARK_CONTAINER=""
if docker ps | grep -q "spark-master"; then
    SPARK_CONTAINER="spark-master"
elif docker ps | grep -q "docker-spark-1"; then
    SPARK_CONTAINER="docker-spark-1"
elif docker ps | grep -q "spark"; then
    SPARK_CONTAINER=$(docker ps --format "{{.Names}}" | grep spark | head -1)
else
    echo "Error: No Spark container is running!"
    echo "Available containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}"
    exit 1
fi

echo "Using Spark container: $SPARK_CONTAINER"

# Submit Spark job to run in local mode (since we have a single Spark container)
echo "Submitting Spark job in local mode..."
docker exec -it $SPARK_CONTAINER spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.driver.host=localhost \
    /opt/bitnami/spark/streaming/spark_streaming_job.py

echo "Spark Streaming Job submitted successfully!"