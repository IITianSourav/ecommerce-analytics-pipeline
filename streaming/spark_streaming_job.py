"""
Spark Streaming Job for E-commerce Analytics Pipeline
====================================================

This module processes real-time e-commerce events from Kafka using PySpark Structured Streaming.
It performs real-time aggregations and writes results to both console and file storage.

Features:
- Reads streaming data from Kafka topic 'ecommerce_transactions'
- Performs real-time aggregations (revenue, order counts, product popularity)
- Handles late-arriving data with watermarking
- Outputs to multiple sinks (console, JSON files, Parquet files)
- Includes error handling and monitoring
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as spark_sum, count, avg, 
    max as spark_max, min as spark_min, current_timestamp,
    date_format, hour, dayofweek, when, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceStreamProcessor:
    """Handles real-time processing of e-commerce transaction streams."""
    
    def __init__(self, kafka_servers="localhost:9092", topic_name="ecommerce_transactions"):
        self.kafka_servers = kafka_servers
        self.topic_name = topic_name
        self.spark = None
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary output directories."""
        os.makedirs("streaming/data/checkpoints", exist_ok=True)
        os.makedirs("streaming/data/output", exist_ok=True)
        os.makedirs("data/processed/streaming", exist_ok=True)
        
    def create_spark_session(self):
        """Initialize Spark session with Kafka dependencies."""
        try:
            self.spark = SparkSession.builder \
                .appName("EcommerceStreamingAnalytics") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            return False
    
    def define_schema(self):
        """Define the schema for incoming JSON events."""
        return StructType([
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("user_segment", StringType(), True),
            StructField("discount_applied", DoubleType(), True)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka topic."""
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.topic_name) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Successfully connected to Kafka topic: {self.topic_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from Kafka: {e}")
            raise
    
    def parse_json_data(self, kafka_df):
        """Parse JSON data from Kafka messages."""
        schema = self.define_schema()
        
        # Parse JSON and extract timestamp
        parsed_df = kafka_df \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_time", 
                       col("timestamp").cast(TimestampType())) \
            .withColumn("processing_time", current_timestamp()) \
            .filter(col("event_time").isNotNull())
        
        # Add derived columns
        enriched_df = parsed_df \
            .withColumn("revenue", col("price") * col("quantity") - col("discount_applied")) \
            .withColumn("hour_of_day", hour("event_time")) \
            .withColumn("day_of_week", dayofweek("event_time")) \
            .withColumn("date", date_format("event_time", "yyyy-MM-dd"))
        
        return enriched_df
    
    def create_real_time_aggregations(self, df):
        """Create various real-time aggregations with watermarking."""
        
        # Set watermark for handling late data (5 minutes)
        watermarked_df = df.withWatermark("event_time", "5 minutes")
        
        # 1. Revenue aggregation by 1-minute windows
        revenue_agg = watermarked_df \
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("revenue").alias("avg_order_value"),
                spark_max("revenue").alias("max_order_value"),
                spark_min("revenue").alias("min_order_value")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # 2. Product popularity by 2-minute windows
        product_popularity = watermarked_df \
            .groupBy(
                window(col("event_time"), "2 minutes"),
                col("product"),
                col("category")
            ) \
            .agg(
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("revenue").alias("product_revenue"),
                count("order_id").alias("order_frequency")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # 3. User segment analysis by 3-minute windows
        user_segment_agg = watermarked_df \
            .groupBy(
                window(col("event_time"), "3 minutes"),
                col("user_segment")
            ) \
            .agg(
                spark_sum("revenue").alias("segment_revenue"),
                count("user_id").alias("active_users"),
                avg("revenue").alias("avg_revenue_per_user")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # 4. Hourly trends (for dashboard)
        hourly_trends = watermarked_df \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("hour_of_day")
            ) \
            .agg(
                spark_sum("revenue").alias("hourly_revenue"),
                count("order_id").alias("hourly_orders")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return {
            "revenue_agg": revenue_agg,
            "product_popularity": product_popularity,
            "user_segment_agg": user_segment_agg,
            "hourly_trends": hourly_trends
        }
    
    def setup_output_sinks(self, aggregations):
        """Setup multiple output sinks for the streaming data."""
        queries = []
        
        # 1. Console output for monitoring
        console_query = aggregations["revenue_agg"] \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 20) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        queries.append(("console_revenue", console_query))
        
        # 2. JSON files for raw aggregated data
        json_query = aggregations["product_popularity"] \
            .writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "streaming/data/output/product_popularity") \
            .option("checkpointLocation", "streaming/data/checkpoints/product_popularity") \
            .trigger(processingTime='1 minute') \
            .start()
        
        queries.append(("json_products", json_query))
        
        # 3. Parquet files for data warehouse
        parquet_query = aggregations["user_segment_agg"] \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/processed/streaming/user_segments") \
            .option("checkpointLocation", "streaming/data/checkpoints/user_segments") \
            .trigger(processingTime='2 minutes') \
            .start()
        
        queries.append(("parquet_segments", parquet_query))
        
        # 4. Memory sink for testing/debugging
        memory_query = aggregations["hourly_trends"] \
            .writeStream \
            .outputMode("update") \
            .format("memory") \
            .queryName("hourly_trends_table") \
            .trigger(processingTime='1 minute') \
            .start()
        
        queries.append(("memory_trends", memory_query))
        
        return queries
    
    def monitor_streams(self, queries):
        """Monitor streaming queries and handle errors."""
        try:
            logger.info(f"Started {len(queries)} streaming queries")
            
            # Wait for all queries and monitor their status
            for name, query in queries:
                logger.info(f"Query '{name}' is active: {query.isActive}")
                
                # Log progress periodically
                if query.lastProgress:
                    progress = query.lastProgress
                    logger.info(f"Query '{name}' progress: "
                              f"inputRowsPerSecond={progress.get('inputRowsPerSecond', 0)}, "
                              f"processedRowsPerSecond={progress.get('processedRowsPerSecond', 0)}")
            
            # Keep the main thread alive
            for name, query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping streams...")
            self.stop_all_streams(queries)
        except Exception as e:
            logger.error(f"Error in stream monitoring: {e}")
            self.stop_all_streams(queries)
            raise
    
    def stop_all_streams(self, queries):
        """Gracefully stop all streaming queries."""
        for name, query in queries:
            try:
                if query.isActive:
                    query.stop()
                    logger.info(f"Stopped query: {name}")
            except Exception as e:
                logger.error(f"Error stopping query {name}: {e}")
    
    def run_streaming_pipeline(self):
        """Main method to run the complete streaming pipeline."""
        logger.info("Starting E-commerce Streaming Analytics Pipeline...")
        
        try:
            # Initialize Spark session
            if not self.create_spark_session():
                raise Exception("Failed to initialize Spark session")
            
            # Read from Kafka
            kafka_stream = self.read_kafka_stream()
            
            # Parse JSON data
            parsed_stream = self.parse_json_data(kafka_stream)
            
            # Create aggregations
            aggregations = self.create_real_time_aggregations(parsed_stream)
            
            # Setup output sinks
            queries = self.setup_output_sinks(aggregations)
            
            # Monitor streams
            self.monitor_streams(queries)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main entry point for the streaming application."""
    # Configuration
    KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
    TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_transactions")
    
    # Initialize and run processor
    processor = EcommerceStreamProcessor(
        kafka_servers=KAFKA_SERVERS,
        topic_name=TOPIC_NAME
    )
    
    try:
        processor.run_streaming_pipeline()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()