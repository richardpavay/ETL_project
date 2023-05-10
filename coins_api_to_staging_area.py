# importing libraries
import pandas as pd
import urllib.request, urllib.parse, urllib.error
import json
import mysql.connector
from sqlalchemy import create_engine
from datetime import date

# Setting up API parameters
serviceurl = 'https://api.coinstats.app/public/v1/coins?'
currency = 'USD'
parms = dict()
parms[currency] = currency
url = serviceurl + urllib.parse.urlencode(parms)

# connecting to API, retrieving JSON
uh = urllib.request.urlopen(url)
data = uh.read().decode()
js = json.loads(data)
print(js)

# extracting Data from JSON, transforming it to python dictionary,the to pandas dataframe
result = dict()
c = list()
p = list()
d = list()
current_date = date.today()
for x in js['coins']:
    c.append(x['name'])
    p.append(x['price'])
    d.append(current_date)

result['coin'] = c
result['price'] = p
result['date'] = d
df = pd.DataFrame(data=result)

# Setting up MySQL connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password='Placeholder2023'
)

conn = create_engine('mysql+mysqldb://root:Placeholder2023@localhost/coins')

# Inserting DataFrame to MySQL database
df.to_sql(con=conn,name='coin_prices',if_exists='append',index=False)
# Removing comment v32