import unittest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock
from pyspark.sql.functions import col

class TestIncrementalLoad(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession
        cls.spark = SparkSession.builder.master("local").appName("Incrementalload").enableHiveSupport().getOrCreate()
    
    @patch('pyspark.sql.SparkSession.read')
    def test_query_format(self, mock_read):
        # Mock the Cumulative_Volume value
        last_Cumulative_Volume = 36805900.826118246
        query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > {}".format(last_Cumulative_Volume)

        # Assert that the query is formed correctly
        self.assertIn(str(last_Cumulative_Volume), query)
        
        # Simulate a DataFrame being returned from the database (Mocking)
        mock_data = MagicMock()
        mock_data.show.return_value = None  # Mocking the show() function
        
        mock_read.return_value = mock_data
        
        # Simulate reading data from PostgreSQL (this won't actually hit the database)
        new_data = self.spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", "consultants") \
            .option("password", "WelcomeItc@2022") \
            .option("query", query) \
            .load()

        new_data.show()
        mock_read.assert_called_once()  # Ensure the read function was called once with the query
    
    @patch('pyspark.sql.DataFrame.write')
    def test_data_write(self, mock_write):
        # Create a mock DataFrame
        df = self.spark.createDataFrame([(1, "test")], ["id", "name"])
        
        # Simulate writing data to Hive
        df.write.mode("append").saveAsTable("project2024.bitcoin_inc_team")
        
        # Verify if write was called with correct parameters
        mock_write.assert_called_once_with(mode="append")
        
    @patch('pyspark.sql.SparkSession.read')
    def test_data_loading(self, mock_read):
        # Simulate the data returned by the read method
        mock_data = MagicMock()
        mock_data.show.return_value = None
        mock_data.collect.return_value = [(36805900.826118246, "test_data")]  # Mocked data
        
        mock_read.return_value = mock_data
        
        # Load new data (mocking the read process)
        query = "SELECT * FROM bitcoin_2025 WHERE \"Cumulative_Volume\" > 36805900.826118246"
        new_data = self.spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", "consultants") \
            .option("password", "WelcomeItc@2022") \
            .option("query", query) \
            .load()
        
        # Simulate show method and verify if data is loaded
        new_data.show()
        mock_data.show.assert_called_once()  # Ensure show was called
        self.assertEqual(len(new_data.collect()), 1)  # Ensure one row is returned

if __name__ == '__main__':
    unittest.main()
