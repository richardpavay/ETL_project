# importing libraries
import pandas as pd
import requests
from sqlalchemy import create_engine

# setting up sql connection
CONN = create_engine('mysql+mysqldb://root:Placeholder2023@localhost/coins')

# Setting up API connection, Retrieving JSON
# key = '6ZWN2TPRC621WDV7'
STOCK_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=6ZWN2TPRC621WDV7'
r = requests.get(STOCK_URL)
stock_json = r.json()

# Extracting data from JSON
today = stock_json['Meta Data']['3. Last Refreshed']
stock_price = stock_json['Time Series (Daily)'][today]['1. open']
stock_name = stock_json['Meta Data']['2. Symbol']

# Transforming data to Python dictionary, the to pandas dataframe
dict_stock = {'stock' : [stock_name], 'price' : [stock_price], 'date' : [today]}
df_stock = pd.DataFrame(dict_stock)
# print(df_stock)

# inserting data to MYSQL database
df_stock.to_sql(con=CONN,name='stock_prices',if_exists='append',index=False)
