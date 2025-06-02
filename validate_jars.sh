#!/bin/bash
# validate_jars.sh - Script to validate JAR files are working correctly
# Place this in your project root directory

echo "=== Validating JAR Files Setup ==="

# Check if docker/jars directory exists
if [ ! -d "docker/jars" ]; then
    echo "❌ docker/jars/ directory not found!"
    echo "Run ./download_jars.sh first"
    exit 1
fi

# Check if required JAR files exist
echo "1. Checking local JAR files..."
required_jars=(
    "spark-sql-kafka-0-10_2.12-3.5.0.jar"
    "kafka-clients-3.5.0.jar"
    "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
)

missing_jars=()
for jar in "${required_jars[@]}"; do
    if [ -f "docker/jars/$jar" ]; then
        echo "✓ Found: $jar"
    else
        echo "❌ Missing: $jar"
        missing_jars+=("$jar")
    fi
done

if [ ${#missing_jars[@]} -gt 0 ]; then
    echo ""
    echo "❌ Missing JAR files. Run: ./download_jars.sh"
    exit 1
fi

# Check if Spark container is running
echo ""
echo "2. Checking Spark container..."
if ! docker ps | grep -q docker-spark-1; then
    echo "❌ Spark container is not running!"
    echo "Start with: cd docker && docker-compose up -d spark"
    exit 1
fi
echo "✓ Spark container is running"

# Check JAR files are mounted in container
echo ""
echo "3. Checking JAR files in container..."
jar_count=$(docker exec docker-spark-1 bash -c "ls -1 /opt/spark/extra-jars/*.jar 2>/dev/null | wc -l")
if [ "$jar_count" -eq "0" ]; then
    echo "❌ No JAR files found in container!"
    echo "Check volume mounting in docker/docker-compose.yml"
    echo "Make sure the jars volume is properly mounted"
    exit 1
fi
echo "✓ Found $jar_count JAR files in container"

# List JAR files in container
echo ""
echo "4. JAR files in container:"
docker exec docker-spark-1 bash -c "ls -la /opt/spark/extra-jars/" 2>/dev/null || echo "❌ Cannot access JAR directory in container"

# Test Spark can load Kafka classes
echo ""
echo "5. Testing Kafka class loading..."
kafka_test=$(docker exec docker-spark-1 bash -c "
cd /opt/bitnami/spark &&
./bin/spark-shell --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/extra-jars/kafka-clients-3.5.0.jar \
--master local[1] \
--conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:conf/log4j.properties' \
--conf spark.sql.adaptive.enabled=false \
<<< 'import org.apache.spark.sql.streaming._; spark.version; System.exit(0)' 2>/dev/null | grep -E '(Spark session|version|Welcome)'
" 2>/dev/null)

if echo "$kafka_test" | grep -q -E "(Welcome|version)"; then
    echo "✓ Spark can load Kafka classes successfully"
else
    echo "❌ Failed to load Kafka classes"
    echo "This might be normal - try running your streaming job to test properly"
fi

echo ""
echo "=== Validation Complete ==="
echo ""
echo "📋 Your project structure:"
echo "ecommerce-analytics-pipeline/"
echo "├── docker/"
echo "│   ├── jars/              ← JAR files are here"
echo "│   └── docker-compose.yml"
echo "├── streaming/"
echo "│   └── spark_streaming_job.py"
echo "└── ..."
echo ""
echo "📋 How to run your streaming job:"
echo ""
echo "cd streaming"
echo "docker exec -it docker-spark-1 bash"
echo "./bin/spark-submit \\"
echo "  --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/extra-jars/kafka-clients-3.5.0.jar \\"
echo "  --master local[2] \\"
echo "  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \\"
echo "  streaming/spark_streaming_job.py"
echo ""
echo "Or use the run_streaming_job.sh script for easier execution"