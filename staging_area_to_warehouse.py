import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# setting up MySQL connection
CONN_MYSQL = create_engine('mysql+mysqldb://root:Placeholder2023@localhost/coins')
CONN_PG = create_engine('postgresql+psycopg2://postgres:admin@localhost/warehouse')
# Querying, Grouping, Transforming to dfs
# Getting distinct coin names, transforming into df
coin_names = 'SELECT coin FROM coin_prices GROUP BY coin'
df_coin_names = pd.read_sql(coin_names, CONN_MYSQL)

# UPSERTING coins to postgreSQL 
for row in df_coin_names['coin']:
    value = '\'' + row + '\''
    qry = 'INSERT INTO crypto (crypto_name) VALUES (' + value +') ON CONFLICT (crypto_name) DO NOTHING'
    CONN_PG.execute(qry)

# retrieving crypto prices data
price_query = 'SELECT * FROM coin_prices'
df_coin_prices = pd.read_sql(price_query, CONN_MYSQL)
df_coin_prices = df_coin_prices.rename(columns={'coin': 'crypto_name'})
print(df_coin_prices.columns)
# Select crypto names
crypto_names = 'SELECT * FROM crypto'
df_crypto_names = pd.read_sql(crypto_names, CONN_PG)
# merging dfs on crypto_name
df_prices_keys = df_coin_prices.merge(df_crypto_names, on='crypto_name')
# insert final data into PostgreSQL
df_prices_keys[['crypto_id', 'price', 'date']].to_sql(con=CONN_PG, name='crypto_prices', if_exists='append', index=False)

# retrieving distinct stock names
stock_names = 'SELECT stock FROM stock_prices GROUP BY stock'
df_stock_names = pd.read_sql(stock_names, CONN_MYSQL)

for row in df_stock_names['stock']:
    value = '\'' + row + '\''
    qry = 'INSERT INTO stock (stock_name) VALUES (' + value +') ON CONFLICT (stock_name) DO NOTHING'
    CONN_PG.execute(qry)

# retrieving stock prices data
stock_price_query = 'SELECT * FROM stock_prices'
df_stock_prices = pd.read_sql(stock_price_query, CONN_MYSQL)
df_stock_prices = df_stock_prices.rename(columns={'stock': 'stock_name'})
# Select stock names
existing_stock_names = 'SELECT * FROM stock'
df_existing_stock_names = pd.read_sql(existing_stock_names, CONN_PG)
# merging dfs on crypto_name
df_stock_prices_keys = df_stock_prices.merge(df_existing_stock_names, on='stock_name')
# insert final data into PostgreSQL
df_stock_prices_keys[['stock_id', 'price', 'date']].to_sql(con=CONN_PG, name='stock_prices', if_exists='append', index=False)

