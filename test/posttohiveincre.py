import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Setup Spark session for testing
    spark = SparkSession.builder \
        .appName("IncrementalLoadTest") \
        .master("local") \
        .config("spark.jars", "/path/to/postgresql-<version>.jar") \
        .enableHiveSupport() \
        .getOrCreate()
    yield spark
    spark.stop()

def test_load_data_from_postgresql(spark):
    # Test case for loading data from PostgreSQL
    last_Cumulative_Volume = 36805900.826118246
    query = f"SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {last_Cumulative_Volume}"
    
    new_data = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .option("query", query) \
        .load()

    assert new_data.count() > 0, "No data loaded"
    # Further assertions can be added based on your requirements
