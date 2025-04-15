import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, sum, window
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    POSTGRES_CONFIG,
    WINDOW_DURATION,
    SLIDE_DURATION
)
from src.metrics_collector import MetricsCollector
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoBatchProcessor:
    def __init__(self):
        # Get the absolute path to the project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        postgres_jar = os.path.join(project_root, "postgresql-42.7.3.jar")
        
        logger.info("Initializing Spark session...")
        self.spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master(SPARK_MASTER) \
            .config("spark.jars", postgres_jar) \
            .config("spark.driver.extraClassPath", postgres_jar) \
            .config("spark.executor.extraClassPath", postgres_jar) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.ui.port", "4042") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

        # Set log level to WARN to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
        
        # Initialize metrics collector
        self.metrics_collector = MetricsCollector()

    def run_price_analysis(self):
        """Run batch analysis on price data."""
        start_time = time.time()
        error_count = 0
        
        try:
            logger.info("Reading price data from database...")
            price_df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}") \
                .option("dbtable", "crypto_prices") \
                .option("user", POSTGRES_CONFIG['user']) \
                .option("password", POSTGRES_CONFIG['password']) \
                .load()
            
            logger.info("Price data schema: %s", price_df.schema)
            
            # Ensure timestamp column exists and is in correct format
            if 'timestamp' not in price_df.columns:
                logger.error("Timestamp column not found in price data")
                return None
            
            # Perform window aggregations
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
            
            # Show results
            logger.info("\nPrice Analysis Results:")
            window_agg.show(truncate=False)
            
            # Record metrics
            processing_time = time.time() - start_time
            self.metrics_collector.record_batch_metrics(
                total_records=price_df.count(),
                processing_time=processing_time,
                error_count=error_count
            )
            
            return window_agg
        except Exception as e:
            logger.error("Error analyzing price data: %s", str(e))
            error_count += 1
            return None

    def run_market_analysis(self):
        """Run batch analysis on market data."""
        start_time = time.time()
        error_count = 0
        
        try:
            logger.info("Reading market metrics from database...")
            market_df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}") \
                .option("dbtable", "market_metrics") \
                .option("user", POSTGRES_CONFIG['user']) \
                .option("password", POSTGRES_CONFIG['password']) \
                .load()
            
            logger.info("Market metrics schema: %s", market_df.schema)
            
            # Calculate average changes
            avg_changes = market_df \
                .groupBy("coin_id") \
                .agg(
                    avg("price_change_24h").alias("avg_price_change"),
                    avg("market_cap_change_24h").alias("avg_market_cap_change"),
                    avg("volume_change_24h").alias("avg_volume_change")
                )
            
            # Show results
            logger.info("\nMarket Analysis Results:")
            avg_changes.show(truncate=False)
            
            # Record metrics
            processing_time = time.time() - start_time
            self.metrics_collector.record_batch_metrics(
                total_records=market_df.count(),
                processing_time=processing_time,
                error_count=error_count
            )
            
            return avg_changes
        except Exception as e:
            logger.error("Error analyzing market metrics: %s", str(e))
            error_count += 1
            return None

    def compare_streaming_batch(self):
        """Compare streaming and batch processing results."""
        start_time = time.time()
        error_count = 0
        
        try:
            logger.info("Comparing streaming and batch results...")
            
            # Read streaming results
            streaming_agg = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}") \
                .option("dbtable", "window_aggregations") \
                .option("user", POSTGRES_CONFIG['user']) \
                .option("password", POSTGRES_CONFIG['password']) \
                .load()
            
            # Calculate batch aggregations
            price_df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}") \
                .option("dbtable", "crypto_prices") \
                .option("user", POSTGRES_CONFIG['user']) \
                .option("password", POSTGRES_CONFIG['password']) \
                .load()
            
            batch_agg = price_df \
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
            
            # Join and compare
            comparison = streaming_agg.join(
                batch_agg,
                (streaming_agg.window_start == batch_agg.window.start) & 
                (streaming_agg.window_end == batch_agg.window.end) & 
                (streaming_agg.coin_id == batch_agg.coin_id),
                "inner"
            )
            
            # Calculate differences
            comparison = comparison \
                .withColumn("avg_price_diff", streaming_agg["avg_price"] - batch_agg["avg_price"]) \
                .withColumn("min_price_diff", streaming_agg["min_price"] - batch_agg["min_price"]) \
                .withColumn("max_price_diff", streaming_agg["max_price"] - batch_agg["max_price"]) \
                .withColumn("volume_diff", streaming_agg["total_volume"] - batch_agg["total_volume"])
            
            # Show comparison results
            logger.info("\nStreaming vs Batch Comparison:")
            comparison.select(
                streaming_agg["window_start"],
                streaming_agg["window_end"],
                streaming_agg["coin_id"],
                "avg_price_diff",
                "min_price_diff",
                "max_price_diff",
                "volume_diff"
            ).show(truncate=False)
            
            # Record metrics
            processing_time = time.time() - start_time
            self.metrics_collector.record_batch_metrics(
                total_records=comparison.count(),
                processing_time=processing_time,
                error_count=error_count
            )
            
            return comparison
        except Exception as e:
            logger.error("Error comparing streaming and batch results: %s", str(e))
            error_count += 1
            return None

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
        """Run all batch analyses."""
        try:
            # Run price analysis
            self.run_price_analysis()
            
            # Run market analysis
            self.run_market_analysis()
            
            # Compare streaming and batch results
            self.compare_streaming_batch()
            
            # Generate metrics report
            self.generate_metrics_report()
            
        except Exception as e:
            logger.error("Error in batch processing: %s", str(e))
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = CryptoBatchProcessor()
    processor.run() 
