#!/bin/bash
# download_jars.sh - Script to download required JAR files
# Place this in your project root directory

# Create jars directory in docker folder (where docker-compose.yml is)
mkdir -p docker/jars

echo "=== Downloading Required JAR Files ==="

# Spark version and Scala version
SPARK_VERSION="3.5.0"
SCALA_VERSION="2.12"
KAFKA_VERSION="3.5.0"

# Base URLs
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

# JAR files to download
declare -A JARS=(
    ["spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"]="${MAVEN_CENTRAL}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
    ["kafka-clients-${KAFKA_VERSION}.jar"]="${MAVEN_CENTRAL}/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"
    ["spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"]="${MAVEN_CENTRAL}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
    ["commons-pool2-2.11.1.jar"]="${MAVEN_CENTRAL}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
)

# Download each JAR file
for jar_name in "${!JARS[@]}"; do
    jar_url="${JARS[$jar_name]}"
    
    if [ -f "docker/jars/$jar_name" ]; then
        echo "✓ $jar_name already exists, skipping..."
    else
        echo "Downloading $jar_name..."
        if curl -L -o "docker/jars/$jar_name" "$jar_url"; then
            echo "✓ Successfully downloaded $jar_name"
        else
            echo "Failed to download $jar_name"
            echo "   URL: $jar_url"
        fi
    fi
done

echo ""
echo "=== Download Summary ==="
echo "JAR files in docker/jars/ directory:"
ls -lh docker/jars/

echo ""
echo "=== Next Steps ==="
echo "1. Update docker-compose.yml to mount JAR files"
echo "2. Run: cd docker && docker-compose down"
echo "3. Run: cd docker && docker-compose up -d"
echo "4. Test with: ./validate_jars.sh"