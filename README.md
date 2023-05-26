# ETL_project
The goal of this project is to to engineer and set up a basic, end-to-end ETL process in python and SQL.
The main elements of the project (also please see: overwiev.drawio.png for the architecture design):
   - Coinbase API to retrieve the current dayly price of the top 100 cryptocurrencies (https://help.coinbase.com/en/cloud/api/coinbase)
   - Alphavantage API to retrieve currant and/or historical stock prices of our choosing (in this example using IBM) (https://www.alphavantage.co/documentation/)
   - Local MySQL database serving as staging area (for schema, please see overview.drawio.png)
   - Local PostgreSQL database serving as a data warehouse (for schema, please see overview.drawio.png)
   - Python scripts serving as glue among the above mentioned elements


