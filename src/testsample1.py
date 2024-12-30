import unittest
from data import read_data, preprocess_data, resample_to_daily

class TestBitcoinData(unittest.TestCase):
    def test_read_data(self):
        filepath = r"C://Users//44754\Downloads//newdoc//btcusd.csv"
        data = read_data(filepath)
        self.assertFalse(data.empty, "Data should not be empty")

    def test_preprocess_data(self):
        filepath = r"C://Users//44754\Downloads//newdoc//btcusd.csv"
        data = read_data(filepath)
        processed_data = preprocess_data(data)
        self.assertIn('Price_Range', processed_data.columns, "Price_Range column should exist")
        self.assertIn('MA_Close_10', processed_data.columns, "MA_Close_10 column should exist")

    def test_resample_to_daily(self):
        filepath = r"C://Users//44754\Downloads//newdoc//btcusd.csv"
        data = read_data(filepath)
        processed_data = preprocess_data(data)
        daily_data = resample_to_daily(processed_data)
        self.assertFalse(daily_data.empty, "Daily resampled data should not be empty")

if __name__ == '__main__':
    unittest.main()
