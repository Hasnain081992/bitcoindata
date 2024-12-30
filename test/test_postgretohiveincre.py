from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

@patch("pyspark.sql.DataFrameWriter.saveAsTable")
@patch("pyspark.sql.DataFrameReader.format")
@patch("pyspark.sql.DataFrameReader.option")
@patch("pyspark.sql.DataFrameReader.load")
def test_incremental_load(mock_load, mock_option, mock_format, mock_saveAsTable):
    # Initialize Spark session
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    # Mocking the return of the DataFrame when `.load()` is called
    new_data_mock = MagicMock()
    new_data_mock.show.return_value = None  # Mock the `show` method

    # Mock the methods in the `.read` chain
    mock_format.return_value = mock_option
    mock_option.side_effect = lambda key, value: mock_load if key == "query" else mock_option
    mock_load.return_value = new_data_mock

    # Mock the save method for DataFrame writing to Hive
    mock_saveAsTable.return_value = None

    # Set the last Cumulative_Volume value manually
    last_Cumulative_Volume = 36805900.826118246
    print(f"Max Cumulative_Volume: {last_Cumulative_Volume}")

    # Build the query
    query = f"SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {last_Cumulative_Volume}"

    # Simulate reading data using the query
    new_data = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "WelcomeItc@2022") \
        .option("query", query) \
        .load()

    # Assert that the correct methods were called
    mock_format.assert_called_with("jdbc")
    mock_option.assert_any_call("url", "jdbc:postgresql://18.132.73.146:5432/testdb")
    mock_option.assert_any_call("query", query)
