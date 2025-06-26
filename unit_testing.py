Unit Testing Examples
1. Test Oracle Connector (tests/test_oracle_connector.py)

import pytest
from pyspark.sql import SparkSession
from connectors.oracle_connector import OracleConnector
from utils.logger import Logger
from unittest.mock import patch, MagicMock


class TestOracleConnector:
    """Unit tests for OracleConnector class"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder.master("local[2]").appName("OracleConnectorTest").getOrCreate()
        
    @pytest.fixture
    def logger(self, spark):
        return Logger(spark, "OracleConnectorTest")
        
    @pytest.fixture
    def config(self):
        return {
            "host": "test-host",
            "port": "1521",
            "service_name": "test_service",
            "username": "test_user",
            "password": "test_pass",
            "read_timeout": 60000,
            "connect_timeout": 10000
        }
        
    def test_init_valid_config(self, spark, logger, config):
        """Test initialization with valid config"""
        connector = OracleConnector(spark, config, logger)
        assert connector.jdbc_url == "jdbc:oracle:thin:@test-host:1521/test_service"
        assert connector.connection_properties["user"] == "test_user"
        assert connector.connection_properties["password"] == "test_pass"
        
    def test_init_missing_config(self, spark, logger):
        """Test initialization with missing config parameters"""
        with pytest.raises(ValueError):
            OracleConnector(spark, {}, logger)
            
    @patch("pyspark.sql.DataFrameReader.jdbc")
    def test_read_table(self, mock_jdbc, spark, logger, config):
        """Test read_table method"""
        mock_df = MagicMock()
        mock_jdbc.return_value = mock_df
        
        connector = OracleConnector(spark, config, logger)
        result = connector.read_table("test_table")
        
        assert result == mock_df
        mock_jdbc.assert_called_once()
        
    @patch("pyspark.sql.DataFrameReader.jdbc")
    def test_read_table_with_partitioning(self, mock_jdbc, spark, logger, config):
        """Test read_table with partitioning"""
        mock_df = MagicMock()
        mock_jdbc.return_value = mock_df
        
        connector = OracleConnector(spark, config, logger)
        
        # Mock the min/max value queries
        with patch.object(connector, '_get_min_value', return_value=1), \
             patch.object(connector, '_get_max_value', return_value=100):
            result = connector.read_table("test_table", partition_column="id", num_partitions=10)
            
        assert result == mock_df
        mock_jdbc.assert_called_once()
        call_args = mock_jdbc.call_args[1]
        assert call_args["numPartitions"] == "10"
        assert call_args["partitionColumn"] == "id"

2. Test Customer Transformations (tests/test_customer_transformations.py)

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
from transformations.customer_transformations import CustomerTransformations
from utils.logger import Logger
from datetime import date


class TestCustomerTransformations:
    """Unit tests for CustomerTransformations class"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder.master("local[2]").appName("CustomerTransformationsTest").getOrCreate()
        
    @pytest.fixture
    def logger(self, spark):
        return Logger(spark, "CustomerTransformationsTest")
        
    @pytest.fixture
    def transformer(self, spark, logger):
        return CustomerTransformations(spark, logger)
        
    @pytest.fixture
    def sample_data(self, spark):
        schema = StructType([
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
        
        data = [
            Row(
                customer_id="CUST1001",
                first_name="John",
                last_name="Doe",
                date_of_birth=date(1980, 5, 15),
                ssn="123-45-6789",
                email="john.doe@example.com",
                phone_number="555-123-4567",
                address_line1="123 Main St",
                address_line2="Apt 4B",
                city="New York",
                state="NY",
                zip_code="10001",
                country="USA",
                customer_since=date(2015, 3, 10),
                segment="PREMIUM",
                credit_score=Decimal(750),
                total_assets=Decimal(1500000.00),
                kyc_status="COMPLETE",
                last_updated=date.today()
            )
        ]
        
        return spark.createDataFrame(data, schema)
        
    def test_transform_customer_data(self, transformer, sample_data):
        """Test customer data transformations"""
        transformed_df = transformer.transform_customer_data(sample_data)
        
        # Check new columns
        assert "full_name" in transformed_df.columns
        assert "current_age" in transformed_df.columns
        assert "is_high_value" in transformed_df.columns
        assert "credit_rating" in transformed_df.columns
        
        # Check transformed values
        row = transformed_df.first()
        assert row.full_name == "John Doe"
        assert row.is_high_value == True
        assert row.credit_rating == "Very Good"
        assert row.is_kyc_complete == True
        
    def test_invalid_schema(self, transformer, spark):
        """Test with invalid input schema"""
        invalid_data = spark.createDataFrame([Row(id=1, name="test")], ["id", "name"])
        
        with pytest.raises(ValueError):
            transformer.transform_customer_data(invalid_data)