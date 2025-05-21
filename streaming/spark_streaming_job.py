from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# Create Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("ECommerceKafkaConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", StringType())  # keep as StringType for safe parsing

# Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize, transform, and enrich
df_orders = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .filter(col("price") > 0) \
    .withColumn("category", when(col("product").isin("phone", "laptop", "tablet"), "Electronics")
                            .otherwise("Accessories")) \
    .withColumn("order_value_bucket", when(col("price") < 300, "Low")
                                      .when(col("price") < 700, "Medium")
                                      .otherwise("High"))

# Print schema and write enriched stream to console
df_orders.printSchema()

# Write enriched stream to Parquet files in raw zone
query = df_orders.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/raw/orders") \
    .option("checkpointLocation", "data/checkpoints/orders") \
    .start()

query.awaitTermination()
