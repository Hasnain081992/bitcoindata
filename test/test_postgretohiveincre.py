import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize SparkSession."""
    return SparkSession.builder.master("local").appName("TestIncrementalLoad").getOrCreate()

def test_spark_session(spark):
    """Test if Spark session is initialized."""
    assert spark is not None
    assert spark.sparkContext is not None

def test_query_generation():
    """Test the query generation logic."""
    last_Cumulative_Volume = 36805900.826118246
    query = f"SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {last_Cumulative_Volume}"
    expected_query = 'SELECT * FROM bitcoin_2025 WHERE "Cumulative_Volume" > 36805900.826118246'
    assert query == expected_query

def test_incremental_load(spark):
    """Test incremental data load logic."""
    # Mock data for testing
    mock_data = [
        (1, "2024-01-01", 100, 200, 150, 1000, 36805901.0),
        (2, "2024-01-02", 200, 300, 250, 2000, 36807000.0),
    ]
    columns = ["ID", "Date", "Low", "High", "Close", "Volume", "Cumulative_Volume"]
    new_data = spark.createDataFrame(mock_data, columns)

    # Simulate the write to Hive (use temp view for testing)
    new_data.createOrReplaceTempView("temp_bitcoin")
    assert spark.sql("SELECT COUNT(*) FROM temp_bitcoin").collect()[0][0] == len(mock_data)

def test_write_to_hive(spark):
    """Test data write to Hive."""
    mock_data = [
        (1, "2024-01-01", 100, 200, 150, 1000, 36805901.0),
    ]
    columns = ["ID", "Date", "Low", "High", "Close", "Volume", "Cumulative_Volume"]
    df = spark.createDataFrame(mock_data, columns)

    # Write to Hive as a temporary table for testing
    df.write.mode("overwrite").saveAsTable("test.bitcoin_test_table")

    # Validate that the table exists and contains the expected data
    loaded_data = spark.sql("SELECT * FROM test.bitcoin_test_table")
    assert loaded_data.count() == len(mock_data)
