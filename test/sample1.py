import pytest
import pandas as pd
from your_module import read_data, preprocess_data  # Adjust import based on your actual file name

# Sample file path (adjust the path as needed)
test_file = r"C://Users//44754\Downloads//newdoc//btcusd.csv"

# Fixture to create sample data
@pytest.fixture
def sample_data():
    return pd.read_csv(test_file)

def test_timestamp_conversion(sample_data):
    """Test if the 'Timestamp' column is correctly converted to a 'Datetime' column."""
    data = preprocess_data(sample_data)
    assert 'Datetime' in data.columns  # Ensure 'Datetime' column exists
    assert pd.api.types.is_datetime64_any_dtype(data['Datetime'])  # Ensure the type is datetime

def test_fillna(sample_data):
    """Test if missing values are correctly filled."""
    # Set NaN values in 'Close' column for testing purposes
    sample_data.loc[1, 'Close'] = np.nan
    data = preprocess_data(sample_data)
    assert not data['Close'].isnull().any()  # Ensure there are no missing values

def test_price_range(sample_data):
    """Test if the price range is calculated correctly."""
    data = preprocess_data(sample_data)
    expected_price_range = sample_data['High'] - sample_data['Low']
    assert all(data['Price_Range'] == expected_price_range)

def test_moving_averages(sample_data):
    """Test if the moving averages are calculated."""
    data = preprocess_data(sample_data)
    # Check the first row's moving averages
    assert data['MA_Close_10'].iloc[0] == pytest.approx(sample_data['Close'][:10].mean(), rel=1e-2)
    assert data['MA_Close_30'].iloc[0] == pytest.approx(sample_data['Close'][:30].mean(), rel=1e-2)

def test_daily_return(sample_data):
    """Test if daily return is calculated correctly."""
    data = preprocess_data(sample_data)
    expected_daily_return = sample_data['Close'].pct_change() * 100
    assert all(data['Daily_Return'] == expected_daily_return)

def test_close_increased(sample_data):
    """Test if Close_Increased column is correctly set to 1 or 0."""
    data = preprocess_data(sample_data)
    assert all(data['Close_Increased'] == (sample_data['Close'].diff() > 0).astype(int))

def test_cumulative_volume(sample_data):
    """Test if the cumulative volume is calculated correctly."""
    data = preprocess_data(sample_data)
    expected_cumulative_volume = sample_data['Volume'].cumsum()
    assert all(data['Cumulative_Volume'] == expected_cumulative_volume)
