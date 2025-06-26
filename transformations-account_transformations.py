#Account Transformation Module

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
from models.account import Account
from utils.data_validator import DataValidator
from utils.logger import Logger


class AccountTransformations:
    """Advanced account data transformations with currency conversion"""
    
    def __init__(self, spark, logger: Logger):
        self.spark = spark
        self.logger = logger
        self.validator = DataValidator(logger)
        
    def transform_account_data(self, raw_df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
        """Transform raw account data including currency conversion"""
        try:
            # Validate input schema
            required_schema = StructType([
                StructField("account_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("account_type", StringType(), False),
                StructField("account_subtype", StringType(), True),
                StructField("open_date", DateType(), False),
                StructField("close_date", DateType(), True),
                StructField("status", StringType(), False),
                StructField("currency_code", StringType(), False),
                StructField("current_balance", DecimalType(20, 4), False),
                StructField("available_balance", DecimalType(20, 4), False),
                StructField("hold_balance", DecimalType(20, 4), True),
                StructField("interest_rate", DecimalType(10, 4), True),
                StructField("overdraft_limit", DecimalType(15, 2), True),
                StructField("last_activity_date", DateType(), True),
                StructField("branch_id", StringType(), True),
                StructField("primary_holder", StringType(), True),
                StructField("secondary_holders", StringType(), True),
                StructField("account_manager", StringType(), True),
                StructField("last_updated", TimestampType(), True)
            ])
            
            if not self.validator.validate_schema(raw_df, required_schema):
                raise ValueError("Input DataFrame schema doesn't match required account schema")
                
            # Join with exchange rates
            account_with_rates = raw_df.join(
                F.broadcast(exchange_rates_df),
                raw_df["currency_code"] == exchange_rates_df["from_currency"],
                "left"
            )
            
            # Apply transformations
            transformed_df = account_with_rates \
                .withColumn("is_active", F.col("status") == "ACTIVE") \
                .withColumn("is_closed", F.col("close_date").isNotNull()) \
                .withColumn("account_age_days", F.datediff(F.current_date(), F.col("open_date"))) \
                .withColumn("account_age_years", F.col("account_age_days") / 365.25) \
                .withColumn("usd_current_balance", 
                           F.when(F.col("to_currency") == "USD", 
                                 F.col("current_balance") * F.col("exchange_rate"))
                            .otherwise(F.col("current_balance"))) \
                .withColumn("usd_available_balance", 
                           F.when(F.col("to_currency") == "USD", 
                                 F.col("available_balance") * F.col("exchange_rate"))
                            .otherwise(F.col("available_balance"))) \
                .withColumn("balance_diff", F.col("current_balance") - F.col("available_balance"))) \
                .withColumn("is_overdrawn", F.col("available_balance") < 0) \
                .withColumn("has_overdraft_protection", F.col("overdraft_limit") > 0) \
                .withColumn("record_source", F.lit("SNOWFLAKE_ACCOUNTS")) \
                .withColumn("ingestion_timestamp", F.current_timestamp())
                
            # Apply data quality checks
            transformed_df = self.validator.apply_standard_checks(transformed_df, Account.ID_COLUMNS, Account.NOT_NULL_COLUMNS)
            
            # Select final columns
            final_df = transformed_df.select(
                Account.ID_COLUMNS +
                ["customer_id", "account_type", "account_subtype", "open_date", "close_date", 
                 "is_active", "is_closed", "account_age_days", "account_age_years", "status",
                 "currency_code", "current_balance", "available_balance", "hold_balance",
                 "usd_current_balance", "usd_available_balance", "balance_diff",
                 "interest_rate", "overdraft_limit", "is_overdrawn", "has_overdraft_protection",
                 "last_activity_date", "branch_id", "primary_holder", "secondary_holders",
                 "account_manager", "last_updated", "record_source", "ingestion_timestamp"]
            )
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error transforming account data: {str(e)}")
            raise