from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, TimestampType

# Create Spark session
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
    .add("timestamp", StringType())  # use StringType here to avoid parsing issues

# Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize and transform
df_orders = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Print schema and write to console
df_orders.printSchema()

query = df_orders.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
