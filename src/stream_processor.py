from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min, max, sum, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_BATCH_DURATION,
    WINDOW_DURATION,
    SLIDE_DURATION
)
from utils.database import (
    insert_price_data,
    insert_market_metrics,
    insert_window_aggregation
)
from src.metrics_collector import MetricsCollector
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoStreamProcessor:
    def __init__(self):
        logger.info("Initializing Spark session...")
        self.spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.minBatchesToRetain", "2") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.ui.port", "4041") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.rpc.askTimeout", "600s") \
            .config("spark.rpc.lookupTimeout", "600s") \
            .getOrCreate()
        
        # Set log level to WARN to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
        
        # Initialize metrics collector
        self.metrics_collector = MetricsCollector()
        logger.info("Metrics collector initialized")
        
        # Define schemas for Kafka messages
        self.price_schema = StructType([
            StructField("coin_id", StringType()),
            StructField("price_usd", DoubleType()),
            StructField("market_cap", DoubleType()),
            StructField("volume_24h", DoubleType()),
            StructField("timestamp", LongType())
        ])
        
        self.market_schema = StructType([
            StructField("coin_id", StringType()),
            StructField("price_change_24h", DoubleType()),
            StructField("market_cap_change_24h", DoubleType()),
            StructField("volume_change_24h", DoubleType()),
            StructField("timestamp", LongType())
        ])

    def process_price_stream(self):
        """Process the price data stream."""
        logger.info("Starting price stream processing...")
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPICS['PRICES']) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        logger.info("Reading from Kafka topic: %s", KAFKA_TOPICS['PRICES'])
        
        # Parse JSON and create DataFrame
        price_df = df.select(
            from_json(col("value").cast("string"), self.price_schema).alias("data")
        ).select("data.*")
        
        logger.info("Price data schema: %s", price_df.schema)
        
        # Convert timestamp from milliseconds to seconds and then to timestamp
        price_df = price_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp") / 1000).cast(TimestampType())
        )
        
        # Write raw price data to database
        def write_raw_prices(batch_df, batch_id):
            start_time = time.time()
            error_count = 0
            batch_size = 0
            
            try:
                batch_size = batch_df.count()
                logger.info("Processing raw price batch %d with %d rows", batch_id, batch_size)
                
                for row in batch_df.collect():
                    try:
                        logger.info("Writing raw price for %s: price=%.2f, market_cap=%.2f, volume=%.2f",
                                  row['coin_id'], row['price_usd'], row['market_cap'], row['volume_24h'])
                        insert_price_data(
                            row['coin_id'],
                            row['price_usd'],
                            row['market_cap'],
                            row['volume_24h']
                        )
                        logger.info("Successfully wrote raw price to database")
                    except Exception as e:
                        logger.error("Error writing raw price to database: %s", str(e))
                        error_count += 1
            except Exception as e:
                logger.error("Error processing raw price batch: %s", str(e))
                error_count += 1
            
            # Record metrics regardless of batch size
            processing_time = time.time() - start_time
            self.metrics_collector.record_streaming_metrics(
                batch_size=batch_size,
                processing_time=processing_time,
                error_count=error_count
            )
        
        # Start raw price writing stream
        raw_price_query = price_df.writeStream \
            .foreachBatch(write_raw_prices) \
            .outputMode("append") \
            .trigger(processingTime="90 seconds") \
            .start()
        
        # Window aggregations
        window_agg = price_df \
            .groupBy(
                window("timestamp", f"{WINDOW_DURATION} minutes", f"{SLIDE_DURATION} minutes"),
                "coin_id"
            ) \
            .agg(
                avg("price_usd").alias("avg_price"),
                min("price_usd").alias("min_price"),
                max("price_usd").alias("max_price"),
                sum("volume_24h").alias("total_volume")
            )
        
        # Write window aggregations to database
        def write_window_agg(batch_df, batch_id):
            start_time = time.time()
            error_count = 0
            batch_size = 0
            
            try:
                batch_size = batch_df.count()
                logger.info("Processing window aggregation batch %d with %d rows", batch_id, batch_size)
                
                for row in batch_df.collect():
                    try:
                        logger.info("Writing window aggregation for %s: avg=%.2f, min=%.2f, max=%.2f, volume=%.2f",
                                  row['coin_id'], row['avg_price'], row['min_price'], row['max_price'], row['total_volume'])
                        insert_window_aggregation(
                            row['window']['start'],
                            row['window']['end'],
                            row['coin_id'],
                            row['avg_price'],
                            row['min_price'],
                            row['max_price'],
                            row['total_volume']
                        )
                        logger.info("Successfully wrote window aggregation to database")
                    except Exception as e:
                        logger.error("Error writing window aggregation to database: %s", str(e))
                        error_count += 1
            except Exception as e:
                logger.error("Error processing window aggregation batch: %s", str(e))
                error_count += 1
            
            # Record metrics regardless of batch size
            processing_time = time.time() - start_time
            self.metrics_collector.record_streaming_metrics(
                batch_size=batch_size,
                processing_time=processing_time,
                error_count=error_count
            )
        
        # Start window aggregation writing stream
        window_query = window_agg.writeStream \
            .foreachBatch(write_window_agg) \
            .outputMode("update") \
            .trigger(processingTime="90 seconds") \
            .start()
        
        return raw_price_query, window_query

    def process_market_stream(self):
        """Process the market data stream."""
        logger.info("Starting market data stream processing...")
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPICS['MARKET_DATA']) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("Reading from Kafka topic: %s", KAFKA_TOPICS['MARKET_DATA'])
        
        # Parse JSON and create DataFrame
        market_df = df.select(
            from_json(col("value").cast("string"), self.market_schema).alias("data")
        ).select("data.*")
        
        logger.info("Market data schema: %s", market_df.schema)
        
        # Convert timestamp from milliseconds to seconds and then to timestamp
        market_df = market_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp") / 1000).cast(TimestampType())
        )
        
        # Write to database
        def write_to_db(batch_df, batch_id):
            start_time = time.time()
            error_count = 0
            batch_size = 0
            
            try:
                batch_size = batch_df.count()
                logger.info("Processing market data batch %d with %d rows", batch_id, batch_size)
                
                for row in batch_df.collect():
                    try:
                        logger.info("Writing market metrics for %s: price_change=%.2f, market_cap_change=%.2f, volume_change=%.2f",
                              row['coin_id'], row['price_change_24h'], row['market_cap_change_24h'], row['volume_change_24h'])
                        insert_market_metrics(
                            row['coin_id'],
                            row['price_change_24h'],
                            row['market_cap_change_24h'],
                            row['volume_change_24h']
                        )
                        logger.info("Successfully wrote market metrics to database")
                    except Exception as e:
                        logger.error("Error writing market metrics to database: %s", str(e))
                        error_count += 1
            except Exception as e:
                logger.error("Error processing market data batch: %s", str(e))
                error_count += 1
            
            # Record metrics regardless of batch size
            processing_time = time.time() - start_time
            self.metrics_collector.record_streaming_metrics(
                batch_size=batch_size,
                processing_time=processing_time,
                error_count=error_count
            )
        
        query = market_df.writeStream \
            .foreachBatch(write_to_db) \
            .outputMode("append") \
            .trigger(processingTime=f"{SPARK_BATCH_DURATION} seconds") \
            .start()
        
        return query

    def generate_metrics_report(self):
        """Generate a comprehensive metrics report."""
        self.metrics_collector.generate_report()
        
        # Compare data consistency
        consistency_metrics = self.metrics_collector.compare_consistency(self.spark)
        if consistency_metrics:
            logger.info("\nData Consistency Metrics:")
            logger.info(f"Price Difference: {consistency_metrics['price_difference_percent']:.2f}%")
            logger.info(f"Volume Difference: {consistency_metrics['volume_difference_percent']:.2f}%")
            logger.info(f"Streaming Records: {consistency_metrics['streaming_records']}")
            logger.info(f"Batch Records: {consistency_metrics['batch_records']}")

    def run(self):
        """Start all stream processing jobs."""
        logger.info("Starting stream processing...")
        raw_price_query, window_query = self.process_price_stream()
        market_query = self.process_market_stream()
        
        logger.info("Stream processing started. Waiting for termination...")
        raw_price_query.awaitTermination()
        window_query.awaitTermination()
        market_query.awaitTermination()

        self.generate_metrics_report()

if __name__ == "__main__":
    processor = CryptoStreamProcessor()
    processor.run() 
