# Snowflake Connector
from pyspark.sql import SparkSession
from typing import Dict, Optional
from pyspark.sql.dataframe import DataFrame
from connectors.base_connector import BaseConnector
from utils.logger import Logger


class SnowflakeConnector(BaseConnector):
    """Advanced Snowflake Connector with optimized read/write capabilities"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, str], logger: Logger):
        super().__init__(spark, config, logger)
        self._validate_snowflake_config(config)
        self.sf_options = {
            "sfURL": f"{config['account']}.snowflakecomputing.com",
            "sfUser": config["username"],
            "sfPassword": config["password"],
            "sfDatabase": config["database"],
            "sfSchema": config["schema"],
            "sfWarehouse": config["warehouse"],
            "sfRole": config.get("role", ""),
            "sfCompress": "on",
            "sfSSL": "on"
        }
        
    def _validate_snowflake_config(self, config: Dict[str, str]) -> None:
        """Validate Snowflake configuration parameters"""
        required_keys = {"account", "username", "password", "database", "schema", "warehouse"}
        if not all(key in config for key in required_keys):
            raise ValueError(f"Missing required Snowflake config parameters. Required: {required_keys}")
            
    def read_table(self, table_name: str, query: Optional[str] = None) -> DataFrame:
        """Read data from Snowflake table or execute query"""
        try:
            options = self.sf_options.copy()
            if query:
                options["query"] = query
                self.logger.info(f"Executing Snowflake query: {query}")
            else:
                options["dbtable"] = table_name
                self.logger.info(f"Reading Snowflake table: {table_name}")
                
            return self.spark.read.format("net.snowflake.spark.snowflake").options(**options).load()
            
        except Exception as e:
            self.logger.error(f"Error reading from Snowflake: {str(e)}")
            raise
            
    def write_table(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        """Write data to Snowflake table"""
        try:
            options = self.sf_options.copy()
            options["dbtable"] = table_name
            self.logger.info(f"Writing to Snowflake table {table_name} in {mode} mode")
            
            df.write.format("net.snowflake.spark.snowflake").options(**options).mode(mode).save()
            
        except Exception as e:
            self.logger.error(f"Error writing to Snowflake table {table_name}: {str(e)}")
            raise