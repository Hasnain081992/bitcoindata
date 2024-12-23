#importing pandas libraries and numpy
import pandas as pd
import numpy as np



# read data in python
data1 = pd.read_csv(r"C://Users//44754\Downloads//newdoc//btcusd.csv")
# to see first 5 rows
data1.head(5)
# convert timestamp into redable data format
data1['Datetime'] = pd.to_datetime(data1['Timestamp'], unit = 's')
print(data1['Datetime'])

#set datetime into index
#data1.set_index('Datetime',inplace = False)
#print(data1.set_index)

#Drop the original Timestamp column
#data1.drop(columns=['Timestamp'], inplace=True)
#print(data1.drop)

    #Fill missing values with the forward-fill method
data1.fillna(method='ffill', inplace=True)
print(data1.fillna)

# Calculate the price range (High - Low) and add as a new column
data1['Price_Range'] = data1['High'] - data1['Low']
print(data1['Price_Range'])

#Add a 10-period moving average of the Close price
data1['MA_Close_10'] = data1['Close'].rolling(window=10).mean()
print(data1['MA_Close_10'])


#Add a 30-period moving average of the Close price
data1['MA_Close_30'] = data1['Close'].rolling(window=30).mean()
print(data1['MA_Close_30'])

#Calculate the daily return percentage
data1['Daily_Return'] = data1['Close'].pct_change() * 100
print(data1['Daily_Return'])

#add a column indicating if the Close price increased (1) or decreased (0)
data1['Close_Increased'] = (data1['Close'].diff() > 0).astype(int)
print(data1['Close_Increased'])

#Resample data to daily frequency and calculate the mean Close price
daily_data = data1['Close'].resample('D').mean().to_frame(name='Daily_Close_Mean')

#  Add a column for cumulative sum of Volume
data1['Cumulative_Volume'] = data1['Volume'].cumsum()
print(data1['Cumulative_Volume'])

data1.dtypes


