#Customer Transformation Module
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
from models.customer import Customer
from utils.data_validator import DataValidator
from utils.logger import Logger


class CustomerTransformations:
    """Advanced customer data transformations with data quality checks"""
    
    def __init__(self, spark, logger: Logger):
        self.spark = spark
        self.logger = logger
        self.validator = DataValidator(logger)
        
    def transform_customer_data(self, raw_df: DataFrame) -> DataFrame:
        """Apply comprehensive transformations to raw customer data"""
        try:
            # Validate input schema
            required_schema = StructType([
                StructField("customer_id", StringType(), False),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("date_of_birth", DateType(), True),
                StructField("ssn", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone_number", StringType(), True),
                StructField("address_line1", StringType(), True),
                StructField("address_line2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("customer_since", DateType(), True),
                StructField("segment", StringType(), True),
                StructField("credit_score", DecimalType(5,0), True),
                StructField("total_assets", DecimalType(15,2), True),
                StructField("kyc_status", StringType(), True),
                StructField("last_updated", DateType(), True)
            ])
            
            if not self.validator.validate_schema(raw_df, required_schema):
                raise ValueError("Input DataFrame schema doesn't match required customer schema")
                
            # Apply transformations
            transformed_df = raw_df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))) \
                .withColumn("current_age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25) \
                .withColumn("address", F.concat_ws(", ", F.col("address_line1"), F.col("address_line2"), F.col("city"), 
                                                 F.col("state"), F.col("zip_code"), F.col("country"))) \
                .withColumn("is_high_value", F.when(F.col("total_assets") >= 1000000, True).otherwise(False)) \
                .withColumn("credit_rating", 
                           F.when(F.col("credit_score") >= 800, "Excellent")
                            .when(F.col("credit_score") >= 740, "Very Good")
                            .when(F.col("credit_score") >= 670, "Good")
                            .when(F.col("credit_score") >= 580, "Fair")
                            .otherwise("Poor")) \
                .withColumn("customer_tenure_years", F.floor(F.datediff(F.current_date(), F.col("customer_since")) / 365.25)) \
                .withColumn("is_kyc_complete", F.col("kyc_status") == "COMPLETE") \
                .withColumn("record_source", F.lit("ORACLE_CUSTOMERS")) \
                .withColumn("ingestion_timestamp", F.current_timestamp())
                
            # Apply data quality checks
            transformed_df = self.validator.apply_standard_checks(transformed_df, Customer.ID_COLUMNS, Customer.NOT_NULL_COLUMNS)
            
            # Select final columns
            final_df = transformed_df.select(
                Customer.ID_COLUMNS + 
                ["full_name", "date_of_birth", "current_age", "email", "phone_number", "address", 
                 "customer_since", "customer_tenure_years", "segment", "credit_score", "credit_rating",
                 "total_assets", "is_high_value", "kyc_status", "is_kyc_complete", "last_updated",
                 "record_source", "ingestion_timestamp"]
            )
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error transforming customer data: {str(e)}")
            raise