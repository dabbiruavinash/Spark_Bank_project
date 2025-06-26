# Main Execution Script


from pyspark.sql import SparkSession
from jobs.customer_etl_job import CustomerETLJob
from jobs.account_etl_job import AccountETLJob
from jobs.transaction_etl_job import TransactionETLJob
from utils.logger import Logger
import json
import sys


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file"""
    with open(config_path) as f:
        return json.load(f)
        

def main():
    """Main execution function"""
    try:
        # Initialize Spark session with advanced configuration
        spark = SparkSession.builder \
            .appName("BankingDataPlatform") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .getOrCreate()
            
        logger = Logger(spark, "Main")
        logger.info("Starting Banking Data Platform")
        
        # Load configuration
        config = load_config("configs/main_config.json")
        
        # Execute ETL jobs
        jobs = [
            CustomerETLJob(spark, config),
            AccountETLJob(spark, config),
            TransactionETLJob(spark, config)
        ]
        
        for job in jobs:
            logger.info(f"Executing {job.__class__.__name__}")
            job.run()
            
        logger.info("All ETL jobs completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)
        
    finally:
        spark.stop()
        

if __name__ == "__main__":
    main()

