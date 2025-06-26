# AWS Connector
from pyspark.sql import DataFrame
from typing import Dict
from pyspark.sql import SparkSession
from connectors.base_connector import BaseConnector
from utils.logger import Logger


class AWSConnector(BaseConnector):
    """Advanced AWS S3 Connector with optimized data transfer capabilities"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, str], logger: Logger):
        super().__init__(spark, config, logger)
        self._validate_aws_config(config)
        self.s3_bucket = config["s3_bucket"]
        self.aws_access_key = config["aws_access_key_id"]
        self.aws_secret_key = config["aws_secret_access_key"]
        self._configure_aws_credentials()
        
    def _validate_aws_config(self, config: Dict[str, str]) -> None:
        """Validate AWS configuration parameters"""
        required_keys = {"s3_bucket", "aws_access_key_id", "aws_secret_access_key"}
        if not all(key in config for key in required_keys):
            raise ValueError(f"Missing required AWS config parameters. Required: {required_keys}")
            
    def _configure_aws_credentials(self) -> None:
        """Configure AWS credentials in Hadoop configuration"""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", self.aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.aws_secret_key)
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")  # 100MB part size
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "disk")
        
    def read_parquet(self, path: str) -> DataFrame:
        """Read parquet files from S3 with optimized settings"""
        try:
            s3_path = f"s3a://{self.s3_bucket}/{path}"
            self.logger.info(f"Reading parquet data from S3: {s3_path}")
            return self.spark.read.parquet(s3_path)
        except Exception as e:
            self.logger.error(f"Error reading parquet from S3: {str(e)}")
            raise
            
    def write_parquet(self, df: DataFrame, path: str, partition_cols: list = None, mode: str = "overwrite") -> None:
        """Write DataFrame to S3 as parquet with advanced options"""
        try:
            s3_path = f"s3a://{self.s3_bucket}/{path}"
            self.logger.info(f"Writing parquet data to S3: {s3_path}")
            
            writer = df.write \
                .mode(mode) \
                .option("compression", "snappy") \
                .option("parquet.block.size", 256 * 1024 * 1024)  # 256MB row group size
                
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            writer.parquet(s3_path)
            
        except Exception as e:
            self.logger.error(f"Error writing parquet to S3: {str(e)}")
            raise
            
    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Write DataFrame to S3 as Delta Lake format"""
        try:
            s3_path = f"s3a://{self.s3_bucket}/{path}"
            self.logger.info(f"Writing Delta Lake data to S3: {s3_path}")
            
            df.write.format("delta") \
                .mode(mode) \
                .option("compression", "snappy") \
                .save(s3_path)
                
        except Exception as e:
            self.logger.error(f"Error writing Delta Lake to S3: {str(e)}")
            raise