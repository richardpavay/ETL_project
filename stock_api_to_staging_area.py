# importing libraries
import pandas as pd
import mysql.connector
import requests
from sqlalchemy import create_engine

# Setting up API connection, Retrieving JSON
# key = '6ZWN2TPRC621WDV7'
stock_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=6ZWN2TPRC621WDV7'
r = requests.get(stock_url)
stock_json = r.json()

# Extracting data from JSON
today = stock_json['Meta Data']['3. Last Refreshed']
stock_price = stock_json['Time Series (Daily)'][today]['1. open']
stock_name = stock_json['Meta Data']['2. Symbol']

# Transforming data to Python dictionary, the to pandas dataframe
dict_stock = {'stock' : [stock_name], 'price' : [stock_price], 'date' : [today]}
df_stock = pd.DataFrame(dict_stock)
print(df_stock)

# Setting up MySQL connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password='Frissito2019'
)

conn = create_engine('mysql+mysqldb://root:Frissito2019@localhost/coins')

# inserting data to MYSQL database
df_stock.to_sql(con=conn,name='stock_prices',if_exists='append',index=False)