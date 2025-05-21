from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# Create Spark session
spark = SparkSession.builder \
    .appName("BatchOrderAggregator") \
    .master("local[*]") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Load raw orders data
df = spark.read.parquet("data/raw/orders")

# Basic aggregations
agg_product = df.groupBy("product").agg(
    count("*").alias("total_orders"),
    sum("price").alias("total_revenue"),
    avg("price").alias("avg_order_value")
)

agg_user = df.groupBy("user_id").agg(
    count("*").alias("orders_by_user"),
    sum("price").alias("total_spent")
)

# Write to processed zone
agg_product.write.mode("overwrite").parquet("data/processed/aggregated_orders/by_product")
agg_user.write.mode("overwrite").parquet("data/processed/aggregated_orders/by_user")

print("Aggregations completed and written to 'data/processed/aggregated_orders'")
