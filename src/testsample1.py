import unittest
import pandas as pd
from data import read_data, preprocess_data

def test_preprocess_data(self):
    """Test the preprocess_data function."""
    processed_data = preprocess_data(self.sample_data)
    
    # Test that the Datetime column is set as index
    self.assertTrue(isinstance(processed_data.index, pd.DatetimeIndex), "Index is not a DatetimeIndex")
    
    # Test Price_Range calculation
    expected_price_range = processed_data["High"] - processed_data["Low"]
    pd.testing.assert_series_equal(processed_data["Price_Range"], expected_price_range, check_names=False)
    
    # Test Cumulative_Volume calculation
    expected_cumulative_volume = processed_data["Volume"].cumsum()
    pd.testing.assert_series_equal(processed_data["Cumulative_Volume"], expected_cumulative_volume, check_names=False)
