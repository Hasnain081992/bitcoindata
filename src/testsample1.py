import unittest
import pandas as pd
from data import read_data, preprocess_data

class TestDataProcessing(unittest.TestCase):

   
    
    def test_read_data(self):
        """Test the read_data function."""
        # You can use a real file or mock this function if required
        try:
            df = read_data("C://Users//44754//Downloads//newdoc//btcusd.csv")
            self.assertFalse(df.empty, "DataFrame should not be empty")
            self.assertIn("Timestamp", df.columns, "Column 'Timestamp' should be in the DataFrame")
        except FileNotFoundError:
            self.skipTest("Test file not found. Skipping test_read_data.")

    def test_preprocess_data(self):
        """Test the preprocess_data function."""
        processed_data = preprocess_data(self.sample_data)
        
        # Test Datetime column
        self.assertIn("Datetime", processed_data.columns, "Datetime column is missing")
        
        # Test Price_Range calculation
        expected_price_range = processed_data["High"] - processed_data["Low"]
        pd.testing.assert_series_equal(processed_data["Price_Range"], expected_price_range, check_names=False)
        
        # Test Cumulative_Volume calculation
        expected_cumulative_volume = processed_data["Volume"].cumsum()
        pd.testing.assert_series_equal(processed_data["Cumulative_Volume"], expected_cumulative_volume, check_names=False)
        
        # Test moving averages
        self.assertTrue(processed_data["MA_Close_10"].isnull().sum() >= 7, "MA_Close_10 initial NaN values incorrect")
        self.assertTrue(processed_data["MA_Close_30"].isnull().sum() >= 27, "MA_Close_30 initial NaN values incorrect")
        
        # Test Daily_Return
        self.assertTrue("Daily_Return" in processed_data.columns, "Daily_Return column is missing")

if __name__ == "__main__":
    unittest.main()
