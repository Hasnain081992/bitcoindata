import pandas as pd
pip install pytest
import pytest

# Fixture to load the actual btcusd.csv file
@pytest.fixture
def btcusd_data():
    file_path = r"C:\Users\44754\Downloads\btcusd.csv"
    return pd.read_csv(file_path)

# Test for Timestamp conversion to Datetime
def test_convert_timestamp_to_datetime(btcusd_data):
    btcusd_data['Datetime'] = pd.to_datetime(btcusd_data['Timestamp'], unit='s')
    assert btcusd_data['Datetime'].dtype == "datetime64[ns]"

# Test for filling missing values
def test_fill_missing_values(btcusd_data):
    btcusd_data.fillna(method='ffill', inplace=True)
    assert not btcusd_data.isnull().any().any()  # Ensure no missing values remain

# Test for Price Range calculation
def test_price_range(btcusd_data):
    btcusd_data['Price_Range'] = btcusd_data['High'] - btcusd_data['Low']
    assert (btcusd_data['Price_Range'] >= 0).all()  # High must always be >= Low

# Test for Moving Average calculation
def test_moving_average(btcusd_data):
    btcusd_data['MA_Close_10'] = btcusd_data['Close'].rolling(window=10).mean()
    # Ensure the moving average column exists and has NaN values for the initial rows
    assert 'MA_Close_10' in btcusd_data.columns
    assert btcusd_data['MA_Close_10'].isna().sum() == 9  # First 9 rows should be NaN

# Test for Daily Return calculation
def test_daily_return(btcusd_data):
    btcusd_data['Daily_Return'] = btcusd_data['Close'].pct_change() * 100
    # Check that the first row is NaN since pct_change can't calculate it
    assert pd.isna(btcusd_data['Daily_Return'].iloc[0])

# Test for Close Increase Indicator
def test_close_increased(btcusd_data):
    btcusd_data['Close_Increased'] = (btcusd_data['Close'].diff() > 0).astype(int)
    # Ensure the column exists and has only 0 or 1 values
    assert 'Close_Increased' in btcusd_data.columns
    assert set(btcusd_data['Close_Increased'].unique()).issubset({0, 1})

# Test for Cumulative Volume calculation
def test_cumulative_volume(btcusd_data):
    btcusd_data['Cumulative_Volume'] = btcusd_data['Volume'].cumsum()
    # Ensure the cumulative volume is non-decreasing
    assert (btcusd_data['Cumulative_Volume'].diff() >= 0).all()

# Test for Resampling
def test_resample_daily_mean(btcusd_data):
    btcusd_data['Datetime'] = pd.to_datetime(btcusd_data['Timestamp'], unit='s')
    btcusd_data.set_index('Datetime', inplace=True)
    daily_data = btcusd_data['Close'].resample('D').mean()
    # Ensure resampling produces a valid series with mean calculations
    assert daily_data.notnull().all() or daily_data.isnull().all()
