import os
from sqlalchemy import create_engine, text
import pandas as pd
import polars as pl
import re
import datetime
from datetime import datetime as dt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from misc.CustomLogging import Logger, log_fuction

# # Set logging infomation
# current_date = datetime.now(pytz.timezone('Asia/Singapore')).strftime('%Y%m%d')
# logfilename = f"logfiles/Transactions/{current_date}.log"
# loggerobj  = Logger('Transactions',stream='',file=[logfilename])

class transaction_service():

    def __init__(self):
        self.__conn_str = os.environ["COCKROACH_DB"]
        self.__conn = create_engine(self.__conn_str).connect()


    def test_connection_db(self):
        if self.__conn.execute(text("SELECT now()")).fetchall():
            print('##### Successfully Connected to DB #####')
        else:
            print('!!!!! Failed to connect to DB !!!!!')


    def fetch_transactions_target(self):
        self.__transactions_target = pl.read_database(
            query='SELECT * FROM "portfoliomonitoring"."public"."transactions"',
            connection = self.__conn)

        print(f'Target : {self.__transactions_target.shape}')


    def fetch_transactions_source(self):

        scope =  ['https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "./cred/portfolio-367803-a4d1e0b1c6a4.json",
                    scope)

        client = gspread.authorize(credentials)

        port = client.open('StockPortfolioTracker')

        transactions = pl.DataFrame(port.worksheet('Stocks/ETFs Transactions').get_all_records())

        transactions = transactions.select('date','type','security_name','ticker','currency','quantity','price','fees')

        transactions = transactions.with_columns(pl.col('date').str.to_datetime("%m/%d/%Y %H:%M:%S"))

        transactions = transactions.cast({
            'date':pl.Datetime,
            'type':pl.String,
            'security_name':pl.String, 
            'ticker': pl.String,
            'currency': pl.String,
            'quantity':pl.Float64, 
            'price':pl.Float64, 
            'fees':pl.Float64, 
            })

        self.__transactions_source = transactions
        
        print(f'Source : {self.__transactions_source.shape}')


    def insert_delta(self):
        
        if self.__transactions_target.is_empty():
            self.__transactions_source.write_database(
                table_name="transactions",
                connection = self.__conn_str,
                if_table_exists = 'append',
                engine = 'sqlalchemy'
                )

            print(f'Inserting {self.__transactions_source.shape[0]} rows')

        else:

            to_ingest = self.__transactions_source.join(self.__transactions_target,on=['Date','Ticker','Type','Stock'],how='anti')

            if to_ingest.is_empty():
                print('No new rows to ingest')
            else:
                to_ingest.write_database(
                    table_name="transactions",
                    connection = self.__conn_str,
                    if_table_exists = 'append',
                    engine = 'sqlalchemy'
                    )

                print(f'Inserting {to_ingest.shape[0]} rows')



print(f'##### Running Transaction Service #####')
start = dt.now()
tx = transaction_service()
tx.test_connection_db()
print('##### Before Ingestion #####')
tx.fetch_transactions_source()
tx.fetch_transactions_target()
tx.insert_delta()
print('##### After Ingestion #####')
tx.fetch_transactions_source()
end = dt.now()
print(f'Time taken: {end-start}')
