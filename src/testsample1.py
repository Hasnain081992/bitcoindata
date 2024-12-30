import unittest
import pandas as pd


class TestBitcoinData(unittest.TestCase):
    def setUp(self):
        """Set up test data."""
        self.file_path = r"C://Users//44754\Downloads//newdoc//btcusd.csv"
        self.data = read_data(self.file_path)  # Read the actual file
        
    def test_read_data(self):
        """Test the read_data function."""
        df = read_data(self.file_path)
        self.assertIsInstance(df, pd.DataFrame)

    def test_preprocess_data(self):
        """Test the preprocess_data function."""
        processed_df = preprocess_data(self.data.copy())
        self.assertIn('Price_Range', processed_df.columns)
        self.assertIn('MA_Close_10', processed_df.columns)

    def test_resample_to_daily(self):
        """Test the resample_to_daily function."""
        processed_df = preprocess_data(self.data.copy())
        processed_df.set_index('Datetime', inplace=True)  # Ensure Datetime index
        daily_df = resample_to_daily(processed_df)
        self.assertIsInstance(daily_df, pd.DataFrame)
        self.assertIn('Daily_Close_Mean', daily_df.columns)

if __name__ == "__main__":
    unittest.main()
