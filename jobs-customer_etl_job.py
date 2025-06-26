# customer ETL Job
from pyspark.sql import SparkSession
from connectors.oracle_connector import OracleConnector
from connectors.aws_connector import AWSConnector
from transformations.customer_transformations import CustomerTransformations
from utils.logger import Logger
from utils.data_validator import DataValidator
from models.customer import Customer
from datetime import datetime


class CustomerETLJob:
    """End-to-end ETL job for customer data from Oracle to AWS S3"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.logger = Logger(spark, "CustomerETLJob")
        self.validator = DataValidator(self.logger)
        self.oracle_connector = OracleConnector(spark, config["oracle"], self.logger)
        self.aws_connector = AWSConnector(spark, config["aws"], self.logger)
        self.transformer = CustomerTransformations(spark, self.logger)
        
    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            self.logger.info("Starting Customer ETL Job")
            start_time = datetime.now()
            
            # Extract
            self.logger.info("Extracting customer data from Oracle")
            oracle_customers = self._extract_customer_data()
            
            # Transform
            self.logger.info("Transforming customer data")
            transformed_customers = self._transform_customer_data(oracle_customers)
            
            # Load
            self.logger.info("Loading customer data to AWS S3")
            self._load_customer_data(transformed_customers)
            
            # Data Quality Validation
            self.logger.info("Running data quality checks")
            self._validate_output_data()
            
            end_time = datetime.now()
            self.logger.info(f"Customer ETL Job completed successfully in {end_time - start_time}")
            
        except Exception as e:
            self.logger.error(f"Customer ETL Job failed: {str(e)}")
            raise
            
    def _extract_customer_data(self) -> DataFrame:
        """Extract customer data from Oracle with incremental logic"""
        try:
            # Check for incremental load
            last_load_date = self._get_last_load_date("customers")
            
            if last_load_date:
                self.logger.info(f"Incremental load since {last_load_date}")
                query = f"(SELECT * FROM customers WHERE last_updated >= TO_DATE('{last_load_date}', 'YYYY-MM-DD')) tmp"
                return self.oracle_connector.read_table(None, query)
            else:
                self.logger.info("Full load of customer data")
                return self.oracle_connector.read_table("customers", partition_column="customer_id")
                
        except Exception as e:
            self.logger.error(f"Error extracting customer data: {str(e)}")
            raise
            
    def _transform_customer_data(self, raw_customers: DataFrame) -> DataFrame:
        """Apply all transformations to customer data"""
        try:
            # Apply standard transformations
            transformed = self.transformer.transform_customer_data(raw_customers)
            
            # Additional business-specific transformations
            transformed = transformed.withColumn("is_vip", 
                F.when((F.col("total_assets") >= 500000) & (F.col("credit_rating").isin(["Excellent", "Very Good"])), True)
                .otherwise(False))
                
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error transforming customer data: {str(e)}")
            raise
            
    def _load_customer_data(self, customers: DataFrame) -> None:
        """Load transformed customer data to AWS S3"""
        try:
            # Write to Parquet with partitioning
            self.aws_connector.write_parquet(
                customers,
                "banking/customers/",
                partition_cols=["ingestion_date"],
                mode="append"
            )
            
            # Update metadata
            self._update_last_load_date("customers", datetime.now().strftime("%Y-%m-%d"))
            
        except Exception as e:
            self.logger.error(f"Error loading customer data: {str(e)}")
            raise
            
    def _get_last_load_date(self, entity: str) -> Optional[str]:
        """Get last successful load date from metadata"""
        try:
            # In a real implementation, this would query a metadata table
            return None  # Simplified for example
        except Exception as e:
            self.logger.warning(f"Could not retrieve last load date: {str(e)}")
            return None
            
    def _update_last_load_date(self, entity: str, load_date: str) -> None:
        """Update last successful load date in metadata"""
        try:
            # In a real implementation, this would update a metadata table
            pass
        except Exception as e:
            self.logger.warning(f"Could not update last load date: {str(e)}")
            
    def _validate_output_data(self) -> bool:
        """Validate output data meets quality standards"""
        try:
            # Read the data we just wrote
            output_data = self.aws_connector.read_parquet("banking/customers/")
            
            # Check for duplicates
            duplicate_check = self.validator.check_for_duplicates(output_data, Customer.ID_COLUMNS)
            if duplicate_check > 0:
                self.logger.warning(f"Found {duplicate_check} duplicate customer records")
                
            # Check for nulls in critical fields
            null_check = self.validator.check_for_nulls(output_data, Customer.NOT_NULL_COLUMNS)
            if null_check > 0:
                self.logger.warning(f"Found {null_check} null values in required customer fields")
                
            # Check data freshness
            freshness_check = self.validator.check_data_freshness(output_data, "ingestion_timestamp", 24)
            if not freshness_check:
                self.logger.warning("Customer data is not fresh (older than 24 hours)")
                
            return duplicate_check == 0 and null_check == 0 and freshness_check
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            return False