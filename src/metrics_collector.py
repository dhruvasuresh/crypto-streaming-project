import time
import psutil
import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self):
        self.metrics_file = "metrics_data.json"
        self.metrics = self._load_metrics()
        self.start_time = time.time()
    
    def _load_metrics(self) -> Dict:
        """Load metrics from file if it exists, otherwise return default structure."""
        try:
            if os.path.exists(self.metrics_file):
                with open(self.metrics_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading metrics: {str(e)}")
        
        # Return default structure if file doesn't exist or error occurred
        return {
            'streaming': {
                'latency': [],
                'throughput': [],
                'errors': [],
                'resource_usage': [],
                'data_points': []
            },
            'batch': {
                'latency': [],
                'throughput': [],
                'errors': [],
                'resource_usage': [],
                'data_points': []
            }
        }
    
    def _save_metrics(self):
        """Save current metrics to file."""
        try:
            with open(self.metrics_file, 'w') as f:
                json.dump(self.metrics, f)
        except Exception as e:
            logger.error(f"Error saving metrics: {str(e)}")
    
    def record_streaming_metrics(self, batch_size: int, processing_time: float, error_count: int = 0):
        """Record metrics for streaming processing."""
        try:
            current_time = time.time()
            latency = processing_time
            throughput = batch_size / processing_time if processing_time > 0 else 0
            
            self.metrics['streaming']['latency'].append(latency)
            self.metrics['streaming']['throughput'].append(throughput)
            self.metrics['streaming']['errors'].append(error_count)
            self.metrics['streaming']['resource_usage'].append(self._get_resource_usage())
            self.metrics['streaming']['data_points'].append({
                'batch_size': batch_size,
                'processing_time': processing_time,
                'error_count': error_count
            })
            
            # Save metrics after each update
            self._save_metrics()
        except Exception as e:
            logger.error(f"Error recording streaming metrics: {str(e)}")
    
    def record_batch_metrics(self, total_records: int, processing_time: float, error_count: int = 0):
        """Record metrics for batch processing."""
        try:
            current_time = time.time()
            latency = processing_time
            throughput = total_records / processing_time if processing_time > 0 else 0
            
            self.metrics['batch']['latency'].append(latency)
            self.metrics['batch']['throughput'].append(throughput)
            self.metrics['batch']['errors'].append(error_count)
            self.metrics['batch']['resource_usage'].append(self._get_resource_usage())
            self.metrics['batch']['data_points'].append({
                'total_records': total_records,
                'processing_time': processing_time,
                'error_count': error_count
            })
            
            # Save metrics after each update
            self._save_metrics()
        except Exception as e:
            logger.error(f"Error recording batch metrics: {str(e)}")
    
    def _get_resource_usage(self) -> Dict[str, float]:
        """Get current system resource usage."""
        process = psutil.Process()
        return {
            'cpu_percent': process.cpu_percent(),
            'memory_percent': process.memory_percent(),
            'memory_used_gb': process.memory_info().rss / (1024 * 1024 * 1024)
        }
    
    def compare_consistency(self, spark: SparkSession) -> Dict[str, Any]:
        """Compare data consistency between streaming and batch results."""
        try:
            # Read both streaming and batch results
            streaming_df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/crypto_db") \
                .option("dbtable", "window_aggregations") \
                .option("user", "dhruva") \
                .option("password", "Dhruva@2004") \
                .load()
            
            batch_df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/crypto_db") \
                .option("dbtable", "crypto_prices") \
                .option("user", "dhruva") \
                .option("password", "Dhruva@2004") \
                .load()
            
            # Calculate statistics for comparison
            streaming_stats = streaming_df.select(
                avg("avg_price").alias("avg_price"),
                avg("total_volume").alias("avg_volume")
            ).collect()[0]
            
            batch_stats = batch_df.select(
                avg("price_usd").alias("avg_price"),
                avg("volume_24h").alias("avg_volume")
            ).collect()[0]
            
            return {
                'price_difference_percent': abs((streaming_stats.avg_price - batch_stats.avg_price) / batch_stats.avg_price * 100),
                'volume_difference_percent': abs((streaming_stats.avg_volume - batch_stats.avg_volume) / batch_stats.avg_volume * 100),
                'streaming_records': streaming_df.count(),
                'batch_records': batch_df.count()
            }
        except Exception as e:
            logger.error(f"Error comparing consistency: {str(e)}")
            return {}
    
    def generate_report(self) -> None:
        """Generate a comprehensive metrics report."""
        report = {
            'streaming': {
                'avg_latency': sum(self.metrics['streaming']['latency']) / len(self.metrics['streaming']['latency']) if self.metrics['streaming']['latency'] else 0,
                'avg_throughput': sum(self.metrics['streaming']['throughput']) / len(self.metrics['streaming']['throughput']) if self.metrics['streaming']['throughput'] else 0,
                'total_errors': sum(self.metrics['streaming']['errors']),
                'avg_cpu_usage': sum(m['cpu_percent'] for m in self.metrics['streaming']['resource_usage']) / len(self.metrics['streaming']['resource_usage']) if self.metrics['streaming']['resource_usage'] else 0,
                'avg_memory_usage': sum(m['memory_percent'] for m in self.metrics['streaming']['resource_usage']) / len(self.metrics['streaming']['resource_usage']) if self.metrics['streaming']['resource_usage'] else 0
            },
            'batch': {
                'avg_latency': sum(self.metrics['batch']['latency']) / len(self.metrics['batch']['latency']) if self.metrics['batch']['latency'] else 0,
                'avg_throughput': sum(self.metrics['batch']['throughput']) / len(self.metrics['batch']['throughput']) if self.metrics['batch']['throughput'] else 0,
                'total_errors': sum(self.metrics['batch']['errors']),
                'avg_cpu_usage': sum(m['cpu_percent'] for m in self.metrics['batch']['resource_usage']) / len(self.metrics['batch']['resource_usage']) if self.metrics['batch']['resource_usage'] else 0,
                'avg_memory_usage': sum(m['memory_percent'] for m in self.metrics['batch']['resource_usage']) / len(self.metrics['batch']['resource_usage']) if self.metrics['batch']['resource_usage'] else 0
            }
        }
        
        # Print report
        logger.info("\n=== Processing Mode Comparison Report ===")
        logger.info("\nStreaming Mode:")
        logger.info(f"Average Latency: {report['streaming']['avg_latency']:.2f} seconds")
        logger.info(f"Average Throughput: {report['streaming']['avg_throughput']:.2f} records/second")
        logger.info(f"Total Errors: {report['streaming']['total_errors']}")
        logger.info(f"Average CPU Usage: {report['streaming']['avg_cpu_usage']:.2f}%")
        logger.info(f"Average Memory Usage: {report['streaming']['avg_memory_usage']:.2f}%")
        
        logger.info("\nBatch Mode:")
        logger.info(f"Average Latency: {report['batch']['avg_latency']:.2f} seconds")
        logger.info(f"Average Throughput: {report['batch']['avg_throughput']:.2f} records/second")
        logger.info(f"Total Errors: {report['batch']['total_errors']}")
        logger.info(f"Average CPU Usage: {report['batch']['avg_cpu_usage']:.2f}%")
        logger.info(f"Average Memory Usage: {report['batch']['avg_memory_usage']:.2f}%")
        
        # Generate plots
        self._generate_plots()
    
    def _generate_plots(self) -> None:
        """Generate visualization plots for metrics comparison."""
        try:
            # Convert metrics to pandas DataFrames
            streaming_df = pd.DataFrame(self.metrics['streaming']['data_points'])
            batch_df = pd.DataFrame(self.metrics['batch']['data_points'])
            
            # Create plots directory if it doesn't exist
            os.makedirs('plots', exist_ok=True)
            
            # Plot latency comparison
            plt.figure(figsize=(10, 6))
            if not streaming_df.empty:
                sns.lineplot(data=streaming_df, x=streaming_df.index, y='processing_time', label='Streaming')
            if not batch_df.empty:
                sns.lineplot(data=batch_df, x=batch_df.index, y='processing_time', label='Batch')
            plt.title('Processing Latency Comparison')
            plt.xlabel('Batch Number')
            plt.ylabel('Processing Time (seconds)')
            plt.savefig('plots/latency_comparison.png')
            plt.close()
            
            # Plot throughput comparison
            plt.figure(figsize=(10, 6))
            if not streaming_df.empty:
                sns.lineplot(data=streaming_df, x=streaming_df.index, y=streaming_df['batch_size']/streaming_df['processing_time'], label='Streaming')
            if not batch_df.empty:
                sns.lineplot(data=batch_df, x=batch_df.index, y=batch_df['total_records']/batch_df['processing_time'], label='Batch')
            plt.title('Throughput Comparison')
            plt.xlabel('Batch Number')
            plt.ylabel('Records per Second')
            plt.savefig('plots/throughput_comparison.png')
            plt.close()
            
            # Plot resource usage
            streaming_resources = pd.DataFrame(self.metrics['streaming']['resource_usage'])
            batch_resources = pd.DataFrame(self.metrics['batch']['resource_usage'])
            
            plt.figure(figsize=(10, 6))
            if not streaming_resources.empty:
                sns.lineplot(data=streaming_resources, x=streaming_resources.index, y='cpu_percent', label='Streaming CPU')
            if not batch_resources.empty:
                sns.lineplot(data=batch_resources, x=batch_resources.index, y='cpu_percent', label='Batch CPU')
            plt.title('CPU Usage Comparison')
            plt.xlabel('Batch Number')
            plt.ylabel('CPU Usage (%)')
            plt.savefig('plots/cpu_usage_comparison.png')
            plt.close()
            
            plt.figure(figsize=(10, 6))
            if not streaming_resources.empty:
                sns.lineplot(data=streaming_resources, x=streaming_resources.index, y='memory_percent', label='Streaming Memory')
            if not batch_resources.empty:
                sns.lineplot(data=batch_resources, x=batch_resources.index, y='memory_percent', label='Batch Memory')
            plt.title('Memory Usage Comparison')
            plt.xlabel('Batch Number')
            plt.ylabel('Memory Usage (%)')
            plt.savefig('plots/memory_usage_comparison.png')
            plt.close()
            
        except Exception as e:
            logger.error(f"Error generating plots: {str(e)}") 
