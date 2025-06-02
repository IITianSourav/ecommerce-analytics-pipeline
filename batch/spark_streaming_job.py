from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
import time

# Configuration
KAFKA_TOPIC = "ecommerce_transactions"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Define schema to match Kafka producer
schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", StringType())

print("Starting Spark Streaming Job...")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")

# Initialize Spark Session with better configuration
spark = SparkSession.builder \
    .appName("KafkaEcommerceConsumer") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Session created successfully")

try:
    # Read from Kafka with additional options
    print("Setting up Kafka stream...")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.request.timeout.ms", "40000") \
        .load()

    print("Kafka stream configured")

    # Parse the JSON messages and add processing timestamp
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("category", 
                   when(col("product").contains("Phone"), "Electronics")
                   .when(col("product").contains("Laptop"), "Electronics")
                   .when(col("product").contains("Book"), "Books")
                   .when(col("product").contains("Shirt"), "Clothing")
                   .when(col("product").contains("Shoes"), "Clothing")
                   .otherwise("Other")) \
        .withColumn("order_value_bucket",
                   when(col("price") < 50, "Low")
                   .when(col("price") < 200, "Medium")
                   .otherwise("High")) \
        .withColumn("processed_at", current_timestamp())

    print("Data transformation configured")

    # Print schema for debugging
    df_parsed.printSchema()

    # Debug: Print to console first
    debug_query = df_parsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()

    print("Console output stream started")
    
    # Wait a bit to see console output
    time.sleep(30)
    debug_query.stop()

    # Main processing: Write to Parquet files
    print("Starting main processing stream...")
    
    main_query = df_parsed.writeStream \
        .format("parquet") \
        .option("path", "/opt/bitnami/spark/data/processed/") \
        .option("checkpointLocation", "/tmp/checkpoint/") \
        .outputMode("append") \
        .trigger(processingTime='30 seconds') \
        .start()

    print("Main processing stream started. Waiting for data...")
    main_query.awaitTermination()

except Exception as e:
    print(f"Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
    print("Spark session stopped")