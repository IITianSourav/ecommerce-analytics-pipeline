from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ViewAggregatedData") \
    .master("local[*]") \
    .getOrCreate()

print("Aggregated by Product:")
df1 = spark.read.parquet("data/processed/aggregated_orders/by_product")
df1.show()

print("Aggregated by User:")
df2 = spark.read.parquet("data/processed/aggregated_orders/by_user")
df2.show()
