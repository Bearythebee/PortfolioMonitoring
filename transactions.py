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
from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

__conn_str = os.environ["COCKROACH_DB"]
__conn  = create_engine(__conn_str).connect()


@task(name="test_connection_db")
def test_connection_db():
    if __conn.execute(text("SELECT now()")).fetchall():
        print('##### Successfully Connected to DB #####')
    else:
        print('!!!!! Failed to connect to DB !!!!!')

@task(name="fetch_transactions_target")
def fetch_transactions_target():
    __transactions_target = pl.read_database(
        query='SELECT * FROM "portfoliomonitoring"."public"."transactions"',
        connection = __conn)

    print(f'Target : {__transactions_target.shape}')

    return __transactions_target

@task(name="fetch_transactions_source")
def fetch_transactions_source():

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

    __transactions_source = transactions
    
    print(f'Source : {__transactions_source.shape}')

    return __transactions_source

@task(name="insert_delta")
def insert_delta(__transactions_source, __transactions_target):
    
    if __transactions_target.is_empty():
        __transactions_source.write_database(
            table_name="transactions",
            connection = __conn_str,
            if_table_exists = 'append',
            engine = 'sqlalchemy'
            )

        print(f'Inserting {__transactions_source.shape[0]} rows')

    else:

        to_ingest = __transactions_source.join(__transactions_target,on=['date','ticker','type','security_name'],how='anti')

        if to_ingest.is_empty():
            print('No new rows to ingest')
            logger.info('No new rows to ingest')
        else:
            to_ingest.write_database(
                table_name="transactions",
                connection = __conn_str,
                if_table_exists = 'append',
                engine = 'sqlalchemy'
                )

            print(f'Inserting {to_ingest.shape[0]} rows')


@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def transaction_service(name: str = "transaction_service"):
    print(f"Testing Connection to DB")
    test_connection_db()
    __transactions_target = fetch_transactions_target()
    __transactions_source = fetch_transactions_source()
    insert_delta(__transactions_source,__transactions_target)

if __name__ == "__main__":
    transaction_service.from_source(
        source='https://github.com/Bearythebee/PortfolioMonitoring.git',
        entrypoint="transactions.py:transaction_service"
    ).deploy(
        name="transaction_service",
        work_pool_name="PortfolioMonitoring"
        
        )
        # tags=["transaction"],
        # parameters={},
        # schedule=(CronSchedule(cron="0 0 * * *", timezone="Asia/Singapore")),