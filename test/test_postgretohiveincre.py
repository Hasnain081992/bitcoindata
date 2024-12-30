import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from postgretohiveincre2 import last_Cumulative_Volume  # Assume your script is in `your_script.py`

@pytest.fixture
def mock_spark_session():
    # Mock the Spark session creation
    with patch.object(SparkSession, 'builder', autospec=True) as mock_builder:
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session
        yield mock_session

def test_incremental_load(mock_spark_session):
    # Mocking the DataFrame read
    mock_new_data = MagicMock()
    mock_new_data.show.return_value = None  # Mock the `show` method
    
    mock_spark_session.read.format.return_value.option.return_value.load.return_value = mock_new_data
    
    # The original query you'd use
    query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {}".format(last_Cumulative_Volume)
    
    # Simulate your script's process
    new_data = mock_spark_session.read.format("jdbc") \
        .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .option("query", query) \
        .load()

    # Assertions to check if the methods were called
    mock_spark_session.read.format.assert_called_with("jdbc")
    new_data.show.assert_called_once()  # Check that `show` was called on the dataframe

    # Mocking the saveAsTable method
    mock_new_data.write.mode.return_value.saveAsTable.return_value = None  # Mock saveAsTable
    new_data.write.mode("append").saveAsTable("project2024.bitcoin_inc_team")
    
    # Check if saveAsTable was called
    mock_new_data.write.mode.return_value.saveAsTable.assert_called_with("project2024.bitcoin_inc_team")

