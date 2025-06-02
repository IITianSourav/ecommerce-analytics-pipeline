#!/bin/bash
# run_streaming_job.sh - Wrapper script to run Spark streaming jobs
# Place this in your project root directory

# Default values
STREAMING_FILE=""
SPARK_MASTER="local[2]"
USE_PACKAGES="false"

# Help function
show_help() {
    echo "Usage: $0 [OPTIONS] <streaming_job.py>"
    echo ""
    echo "Options:"
    echo "  -f, --file <file>     Spark streaming job file (required)"
    echo "  -m, --master <master> Spark master URL (default: local[2])"
    echo "  -p, --packages        Use --packages instead of local JARs"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -f streaming/spark_streaming_job.py"
    echo "  $0 -f streaming/spark_streaming_job.py -p"
    echo "  $0 spark_streaming_job.py  (if running from streaming/ directory)"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            STREAMING_FILE="$2"
            shift 2
            ;;
        -m|--master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        -p|--packages)
            USE_PACKAGES="true"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            if [ -z "$STREAMING_FILE" ]; then
                STREAMING_FILE="$1"
            fi
            shift
            ;;
    esac
done

# Auto-detect streaming file if not specified
if [ -z "$STREAMING_FILE" ]; then
    if [ -f "streaming/spark_streaming_job.py" ]; then
        STREAMING_FILE="streaming/spark_streaming_job.py"
        echo "📁 Auto-detected streaming file: $STREAMING_FILE"
    elif [ -f "spark_streaming_job.py" ]; then
        STREAMING_FILE="spark_streaming_job.py"
        echo "📁 Auto-detected streaming file: $STREAMING_FILE"
    else
        echo "❌ Error: No streaming job file found"
        echo "   Looking for: streaming/spark_streaming_job.py or spark_streaming_job.py"
        show_help
        exit 1
    fi
fi

# Check if file exists
if [ ! -f "$STREAMING_FILE" ]; then
    echo "❌ Error: File '$STREAMING_FILE' not found"
    exit 1
fi

# Check if Spark container is running
if ! docker ps | grep -q docker-spark-1; then
    echo "❌ Error: Spark container is not running"
    echo "Start with: cd docker && docker-compose up -d spark"
    exit 1
fi

echo "=== Running Spark Streaming Job ==="
echo "File: $STREAMING_FILE"
echo "Master: $SPARK_MASTER"
echo "Using packages: $USE_PACKAGES"
echo ""

# Get the absolute path and filename
FULL_PATH=$(realpath "$STREAMING_FILE")
FILENAME=$(basename "$STREAMING_FILE")
CONTAINER_PATH="/opt/bitnami/spark/jobs/$FILENAME"

# Copy the streaming file to container
echo "📋 Copying streaming job to container..."
docker exec docker-spark-1 mkdir -p /opt/bitnami/spark/jobs
docker cp "$STREAMING_FILE" docker-spark-1:"$CONTAINER_PATH"

# Prepare the spark-submit command
if [ "$USE_PACKAGES" = "true" ]; then
    echo "📦 Using --packages for dependencies..."
    SPARK_CMD="./bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --master $SPARK_MASTER \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
        --conf spark.driver.host=localhost \
        $CONTAINER_PATH"
else
    echo "📁 Using local JAR files..."
    # Check if JAR files exist in container
    jar_check=$(docker exec docker-spark-1 bash -c "ls /opt/spark/extra-jars/*.jar 2>/dev/null | wc -l")
    if [ "$jar_check" -eq "0" ]; then
        echo "⚠️  No local JAR files found, falling back to --packages"
        SPARK_CMD="./bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            --master $SPARK_MASTER \
            --conf spark.sql.adaptive.enabled=false \
            --conf spark.sql.adaptive.coalescePartitions.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
            --conf spark.driver.host=localhost \
            $CONTAINER_PATH"
    else
        jar_files="/opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/extra-jars/kafka-clients-3.5.0.jar,/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
        
        SPARK_CMD="./bin/spark-submit \
            --jars $jar_files \
            --master $SPARK_MASTER \
            --conf spark.sql.adaptive.enabled=false \
            --conf spark.sql.adaptive.coalescePartitions.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
            --conf spark.driver.host=localhost \
            $CONTAINER_PATH"
    fi
fi

# Run the job
echo "🚀 Starting Spark streaming job..."
echo "Command: $SPARK_CMD"
echo ""
echo "Press Ctrl+C to stop the streaming job"
echo ""

docker exec -it docker-spark-1 bash -c "
cd /opt/bitnami/spark &&
$SPARK_CMD
"

echo ""
echo "=== Streaming Job Completed ==="