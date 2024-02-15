import os
from sqlalchemy import create_engine, text
import pandas as pd
import polars as pl
import re
import datetime
from datetime import datetime as dt

import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

        transactions = transactions.drop(['Cost of Transaction',
                        'Cost of Transaction (per unit)', 'Cumulative Cost',
                        'Gains/Losses from Sale', 'Yield on Transaction', 'Cash Flow',
                        'Remarks'])

        transactions = transactions.with_columns(pl.col('Date').str.to_datetime("%m/%d/%Y %H:%M:%S"))

        transactions = transactions.cast({
            'Date':pl.Datetime,
            'Type':pl.String,
            'Stock':pl.String, 
            'Transacted Units':pl.Float64, 
            'Transacted Price (per unit)':pl.Float64, 
            "Total in ticker's currency":pl.Float64, 
            'Fees':pl.Float64, 
            'Stock Split Ratio':pl.Float64, 
            'Prev Row':pl.Int64,
            'Previous Units': pl.Float64,
            'Cumulative Units': pl.Float64,
            'Transacted Value': pl.Float64,
            'Previous Cost': pl.Float64,
            'Ticker': pl.String
            })
        
        def transaction_cost(row: pl.struct) -> pl.Float64:
            if row['Type'] == 'Sell':
                if row['Previous Units'] == 0.0:
                    return 0.0
                else:
                    return (row['Transacted Units']/ row['Previous Units']) * row['Previous Cost']
            else:
                return 0.0

        transactions = transactions.with_columns(pl.struct(['Type','Transacted Units','Previous Units','Previous Cost']).map_elements(transaction_cost).alias('Cost of Transaction'))
        
        def cummulative_cost(row: pl.struct) -> pl.Float64:
            if row['Type'] == 'Buy':
                return row['Previous Cost'] + row['Transacted Value']
            elif row['Type'] == 'Div':
                return row['Previous Cost']
            elif row['Type'] == 'Sell':
                if row['Previous Cost'] <= 0.0:
                    return 0.0
                else:
                    return row['Previous Cost'] - row['Cost of Transaction']
            else:
                return 0.0

        transactions = transactions.with_columns(pl.struct(['Type','Previous Cost','Transacted Value','Cost of Transaction']).map_elements(cummulative_cost).alias('Cummulative Cost'))

        def gainloss_sale(row: pl.struct) -> pl.Float64:
            if row['Type'] == 'Sell':
                return row['Transacted Value'] - row['Cost of Transaction']
            else:
                if row['Type'] == 'Div':
                    return row['Transacted Value']
                else:
                    return 0.0

        transactions = transactions.with_columns(pl.struct(['Type','Transacted Value','Cost of Transaction']).map_elements(gainloss_sale).alias('Gains/Losses from Sale'))

        def cash_flow(row: pl.struct) -> pl.Float64:
            if row['Type'] == 'Buy':
                return -1 * row['Transacted Value']
            else:
                return row['Transacted Value']

        transactions = transactions.with_columns(pl.struct(['Type','Transacted Value']).map_elements(cash_flow).alias('Cash Flow'))

        self.__transactions_source = transactions
        
        print(f'Source : {self.__transactions_source.shape}')
        #print(pd.io.sql.get_schema(self.__transactions_source.to_pandas(),'transactions'))


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
