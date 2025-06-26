# Oracle Connector
from pyspark.sql import SparkSession
from typing import Dict, Optional
from pyspark.sql.dataframe import DataFrame
from connectors.base_connector import BaseConnector
from utils.logger import Logger


class OracleConnector(BaseConnector):
    """Advanced Oracle Database Connector with connection pooling and retry logic"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, str], logger: Logger):
        super().__init__(spark, config, logger)
        self._validate_oracle_config(config)
        self.jdbc_url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}/{config['service_name']}"
        self.connection_properties = {
            "user": config["username"],
            "password": config["password"],
            "driver": "oracle.jdbc.OracleDriver",
            "oracle.jdbc.readTimeout": str(config.get("read_timeout", 60000)),
            "oracle.net.CONNECT_TIMEOUT": str(config.get("connect_timeout", 10000))
        }
        
    def _validate_oracle_config(self, config: Dict[str, str]) -> None:
        """Validate Oracle configuration parameters"""
        required_keys = {"host", "port", "service_name", "username", "password"}
        if not all(key in config for key in required_keys):
            raise ValueError(f"Missing required Oracle config parameters. Required: {required_keys}")
        
    def read_table(self, table_name: str, predicates: Optional[list] = None, 
                  partition_column: Optional[str] = None, num_partitions: int = 10) -> DataFrame:
        """Read data from Oracle table with advanced partitioning options"""
        try:
            read_options = {
                "url": self.jdbc_url,
                "table": table_name,
                "properties": self.connection_properties
            }
            
            if partition_column:
                read_options.update({
                    "partitionColumn": partition_column,
                    "lowerBound": str(self._get_min_value(table_name, partition_column)),
                    "upperBound": str(self._get_max_value(table_name, partition_column)),
                    "numPartitions": str(num_partitions)
                })
                
            if predicates:
                read_options["predicates"] = predicates
                
            self.logger.info(f"Reading Oracle table {table_name} with options: {str(read_options)}")
            return self.spark.read.format("jdbc").options(**read_options).load()
            
        except Exception as e:
            self.logger.error(f"Error reading from Oracle table {table_name}: {str(e)}")
            raise
            
    def _get_min_value(self, table_name: str, column_name: str) -> int:
        """Get minimum value for partitioning"""
        query = f"(SELECT MIN({column_name}) AS min_val FROM {table_name}) tmp"
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        ).first()["min_val"]
        
    def _get_max_value(self, table_name: str, column_name: str) -> int:
        """Get maximum value for partitioning"""
        query = f"(SELECT MAX({column_name}) AS max_val FROM {table_name}) tmp"
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        ).first()["max_val"]