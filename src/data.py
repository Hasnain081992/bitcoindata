#importing pandas libraries and numpy
import pandas as pd
import numpy as np



# read data in python
data1 = pd.read_csv(r"C://Users//44754\Downloads//newdoc//btcusd.csv")
# to see first 5 rows
data1.head(5)
# convert timestamp into redable data format
def preprocess_data(data):
    """Performs preprocessing on the DataFrame."""
    # Convert timestamp to readable datetime
    data['Datetime'] = pd.to_datetime(data['Timestamp'], unit='s')

    # Set Datetime as the index
    data.set_index('Datetime', inplace=True)
    
    # Fill missing values with the forward-fill method
    data.fillna(method='ffill', inplace=True)
    
    # Calculate price range (High - Low)
    data['Price_Range'] = data['High'] - data['Low']
    
    # Add a 10-period moving average of the Close price
    data['MA_Close_10'] = data['Close'].rolling(window=10).mean()
    
    # Add a 30-period moving average of the Close price
    data['MA_Close_30'] = data['Close'].rolling(window=30).mean()
    
    # Calculate daily return percentage
    data['Daily_Return'] = data['Close'].pct_change() * 100
    
    # Add a column indicating if the Close price increased (1) or decreased (0)
    data['Close_Increased'] = (data['Close'].diff() > 0).astype(int)
    
    # Add a column for cumulative sum of Volume
    data['Cumulative_Volume'] = data['Volume'].cumsum()
    
    return data
