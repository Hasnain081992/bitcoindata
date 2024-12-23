#outcome
#Converts timestamps to readable formats.
#Computes key metrics like price range, moving averages, and daily returns.
#Resamples and organizes the data for daily analysis.


import psycopg2
from sqlalchemy import create_engine
import pandas as pd

# PostgreSQL connection details
PUBLIC_IP = "18.132.73.146"  # Replace with your PostgreSQL server IP
USERNAME = "consultants"
PASSWORD = "WelcomeItc@2022"
DB_NAME = "testdb"
PORT = "5432"  # Default PostgreSQL port

# Establish connection using psycopg2
try:
    connection = psycopg2.connect(
        host=PUBLIC_IP,
        database=DB_NAME,
        user=USERNAME,
        password=PASSWORD,
        port=PORT
    )
    print("Connected to the PostgreSQL database successfully!")
except Exception as e:
    print("Failed to connect to the PostgreSQL database!")
    print(e)
    #ok


    # Export the DataFrame to PostgreSQL using SQLAlchemy
#try:
    # Create the SQLAlchemy engine
    #engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{PUBLIC_IP}:{PORT}/{DB_NAME}')
engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
    # Assuming your DataFrame is 'data1'
    #data1 = pd.read_csv(r"C://Users//44754//Downloads//btcusd.csv")  # Replace with your actual DataFrame
   
    
    # Insert the DataFrame into PostgreSQL table 'coin2024'
data1.to_sql('bitcoin_2025', engine, index=False, if_exists='replace')  # Replace 'btcusd_data' with your desired table name


# Close the connection
connection.close()