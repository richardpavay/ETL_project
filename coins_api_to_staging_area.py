# importing libraries
import pandas as pd
import urllib.request, urllib.parse, urllib.error
import json
from sqlalchemy import create_engine
from datetime import date

# Setting up MySQL connection
CONN = create_engine('mysql+mysqldb://root:Placeholder2023@localhost/coins')

# Setting up API parameters
SERVICEURL = 'https://api.coinstats.app/public/v1/coins?'
CURRENCY = 'USD'
parms = dict()
parms[currency] = CURRENCY
url = SERVICEURL + urllib.parse.urlencode(parms)

# connecting to API, retrieving JSON
uh = urllib.request.urlopen(url)
data = uh.read().decode()
js = json.loads(data)
# print(js)

# extracting Data from JSON, transforming it to python dictionary,the to pandas dataframe
result = dict()
names_list = list()
prices_list = list()
dates_list = list()
current_date = date.today()
for x in js['coins']:
    names_list.append(x['name'])
    prices_list.append(x['price'])
    dates_list.append(current_date)

result['coin'] = name_list
result['price'] = prices_list
result['date'] = dates_list
df = pd.DataFrame(data=result)

# Inserting DataFrame to MySQL database
df.to_sql(con=CONN,name='coin_prices',if_exists='append',index=False)
