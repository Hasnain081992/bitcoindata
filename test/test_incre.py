import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

class TestIncrementalLoad(unittest.TestCase):
    
    def setUp(self):
        # Initialize Spark session mock
        self.spark = SparkSession.builder.master("local").appName("Incrementalload").enableHiveSupport().getOrCreate()

        # Define the sample data for new_data DataFrame
        self.new_data_df = pd.DataFrame({
            "Timestamp": [1609459200, 1609545600],
            "High": [30000, 30500],
            "Low": [29500, 30000],
            "Close": [29800, 30200],
            "Volume": [1000, 1100],
            "Cumulative_Volume": [36806000, 36807000]
        })
        
        # Create a mock DataFrame to simulate the new data loaded from PostgreSQL
        self.mock_new_data = self.spark.createDataFrame(self.new_data_df)
    
    @patch("pyspark.sql.DataFrameWriter.saveAsTable")
    @patch("pyspark.sql.DataFrameReader.format")
    @patch("pyspark.sql.DataFrameReader.option")
    @patch("pyspark.sql.DataFrameReader.load")
    def test_incremental_load(self, mock_load, mock_option, mock_format, mock_saveAsTable):
        # Simulate the loading of data from PostgreSQL by mocking `load`
        mock_load.return_value = self.mock_new_data
        
        # Simulate reading data from PostgreSQL
        query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > 36805900.826118246"
        new_data = self.spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", "consultants") \
            .option("password", "WelcomeItc@2022") \
            .option("query", query) \
            .load()

        # Check if the data was loaded correctly by verifying the DataFrame's row count
        self.assertEqual(new_data.count(), 2)  # We mocked the data with 2 rows
        
        # Check if the data matches the expected Cumulative_Volume
        last_Cumulative_Volume = 36805900.826118246
        self.assertTrue(all(new_data.filter(col("Cumulative_Volume") > last_Cumulative_Volume).count() == 2))

        # Simulate writing the new data to Hive
        new_data.write.mode("append").saveAsTable("project2024.bitcoin_inc_team")

        # Check if `saveAsTable` was called
        mock_saveAsTable.assert_called_once_with("project2024.bitcoin_inc_team")

        # Optionally, test additional assertions such as transformations on data if applied
        # Example: Check if a column transformation (like creating a new column) works correctly
        transformed_data = new_data.withColumn("Price_Range", col("High") - col("Low"))
        self.assertTrue("Price_Range" in transformed_data.columns)
    
    def tearDown(self):
        # Clean up after each test
        pass

if __name__ == '__main__':
    unittest.main()