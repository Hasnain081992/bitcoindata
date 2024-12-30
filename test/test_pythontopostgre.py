import unittest
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2


class TestBitcoinDataProcessing(unittest.TestCase):
    def setUp(self):
        # Sample data to simulate the CSV file
        data = {
            "Timestamp": [1609459200, 1609545600, 1609632000],
            "High": [34000, 35000, 36000],
            "Low": [33000, 34000, 35000],
            "Close": [33500, 34500, 35500],
            "Volume": [1000, 1200, 1100]
        }
        self.data1 = pd.DataFrame(data)
        self.data1['Datetime'] = pd.to_datetime(self.data1['Timestamp'], unit='s')
    
    def test_fill_missing_values(self):
        # Introduce a missing value
        self.data1.loc[1, 'High'] = np.nan
        self.data1.fillna(method='ffill', inplace=True)
        self.assertEqual(self.data1.loc[1, 'High'], 34000)

    def test_price_range_calculation(self):
        self.data1['Price_Range'] = self.data1['High'] - self.data1['Low']
        self.assertTrue((self.data1['Price_Range'] == [1000, 1000, 1000]).all())

    def test_moving_average_calculation(self):
        self.data1['MA_Close_10'] = self.data1['Close'].rolling(window=10).mean()
        self.assertTrue(self.data1['MA_Close_10'].isnull().sum() > 0)

    def test_daily_return_calculation(self):
        self.data1['Daily_Return'] = self.data1['Close'].pct_change() * 100
        expected_return = (34500 - 33500) / 33500 * 100
        self.assertAlmostEqual(self.data1.loc[1, 'Daily_Return'], expected_return, places=5)

    def test_close_increased_flag(self):
        self.data1['Close_Increased'] = (self.data1['Close'].diff() > 0).astype(int)
        self.assertEqual(list(self.data1['Close_Increased']), [0, 1, 1])

    def test_postgresql_connection(self):
        # PostgreSQL connection details
        PUBLIC_IP = "18.132.73.146"
        USERNAME = "consultants"
        PASSWORD = "WelcomeItc@2022"
        DB_NAME = "testdb"
        PORT = "5432"

        try:
            connection = psycopg2.connect(
                host=PUBLIC_IP,
                database=DB_NAME,
                user=USERNAME,
                password=PASSWORD,
                port=PORT
            )
            connection.close()
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"PostgreSQL connection failed: {e}")

    def test_data_export_to_postgresql(self):
        engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
        try:
            self.data1.to_sql('bitcoin_2025', engine, index=False, if_exists='replace')
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Export to PostgreSQL failed: {e}")


if __name__ == '__main__':
    unittest.main()
