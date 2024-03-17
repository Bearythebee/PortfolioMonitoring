from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import UpdateMany, UpdateOne
from ScrapePrices import get_prices_full,get_prices_daily
from misc.CustomLogging import Logger, log_fuction
from sqlalchemy import create_engine, text
import polars as pl
import datetime
import pytz
import json
from datetime import datetime
import os
import requests
import multiprocessing
from itertools import repeat
from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule
from prefect.blocks.system import Secret
from prefect.blocks.system import JSON

__uri = Secret.load("mongodb").get()
__mongoclient = MongoClient(__uri, server_api=ServerApi('1'))
__mongodb = __mongoclient.prices
__collection = __mongodb["eod_prices"]
__cockroachdb = create_engine(Secret.load("cockroachdb").get()).connect()

def test_connection():
    '''
    Test Connection with MongoDB
    '''

    try:
        __mongoclient.admin.command('ping')
        print("Successfully connected to MongoDB!")
        __loggerobj.logger.custom_logs("Successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        __loggerobj.logger.custom_logs("Failed to connect to MongoDB!")

# Multiprocessing scraping
def scrape_tasks(ticker):

    output_logs = []

    collection = __collection 

    data = get_prices_full(ticker)
    quotes = data['indicators']['quote'][0]
    
    metadata_columns = ['symbol','currency',
    'instrumentType','exchangeName',
    'firstTradeDate','gmtoffset',
        'timezone', 'exchangeTimezoneName']

    metadata =  {key: data['meta'][key] for key in metadata_columns}

    if metadata['symbol'] == 'SGD=X':
        metadata['symbol'] = 'USD/SGD'

    if metadata['symbol'] == 'HKDSGD=X':
        metadata['symbol'] = 'HKD/SGD'

    symbol_ = metadata['symbol']
    currency_ = metadata['currency']
    exchange_ = metadata['exchangeName']
    instr_type_ = metadata['instrumentType']

    output_logs.append(f'Deleting all records of {ticker}')
    output_logs.append(f"Current no.of.rows for {symbol_}: {len(list(collection.find({'metadata.symbol' : symbol_})))}")
    collection.delete_many({'metadata.symbol' : symbol_})
    output_logs.append(f"no.of.rows after deletion for {symbol_}: {len(list(collection.find({'metadata.symbol' : symbol_})))}")

    fulldata = []

    for i in range(len(data['timestamp'])):
        time_ =  datetime.fromtimestamp(data['timestamp'][i]).replace(hour=0, minute=0, second=0)
        open_ = quotes['open'][i]
        high_ = quotes['high'][i]
        low_ = quotes['low'][i]
        close_ = quotes['close'][i]
        volume_ = quotes['volume'][i]

        row = {
            "timestamp":time_,
            "open":open_,
            "high":high_,
            "low":low_,
            "close":close_,
            "volume":volume_,
            "metadata":metadata
                }


        fulldata.append(row)

    # # print(sorted(fulldata,key=lambda x:x['timestamp']))
    output_logs.append(f"Ingesting {ticker} with {len(fulldata)} items")
    collection.insert_many(sorted(fulldata,key=lambda x:x['timestamp']))

    output_logs.append(f"no.of.rows after ingestion for {ticker}: {len(list(collection.find({'metadata.symbol' : symbol_})))}")

    rank = multiprocessing.current_process()._identity[0]
    output_logs.append(f'Processor {rank}, got ticker={ticker}, and finished updating the database')

    return output_logs


def get_distinct_tickers():
    response = __cockroachdb.execute(text('SELECT distinct "ticker" FROM "portfoliomonitoring"."public"."transactions"')).fetchall()

    __uniquetickers = [x[0] for x in response]

    __uniquetickers.append('SGD=X')
    __uniquetickers.append('HKDSGD=X')

    return __uniquetickers


def fetch_counters():

    current = pl.read_database(
        query='SELECT * FROM "portfoliomonitoring"."public"."securities"',
        connection =  __cockroachdb)

    return current.select(pl.col('sec_id')).to_series()


@task(name='refresh_metada')
def refresh_metadata():

        __uniquetickers = get_distinct_tickers()

        metadata = []

        current = pl.read_database(
            query='SELECT * FROM "portfoliomonitoring"."public"."securities"',
            connection =  __cockroachdb)

        if current.is_empty():
            max_sec_ik = 0
        else:
            max_sec_ik = current.select(pl.col('sec_ik')).max().item()
            

        for security_id in sorted(__uniquetickers):
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{security_id}?interval=1d&range=1d"
            headers = {
                "Access-Control-Allow-Origin":"https://sg.finance.yahoo.com",
                "Content-Type":"application/json;charset=utf-8",
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"\
                            " AppleWebKit/537.36 (KHTML, like Gecko)"\
                            " Chrome/121.0.0.0 Safari/537.36"
                }
            response = requests.get(url,headers=headers)

            try:
            
                data = json.loads(response.text)['chart']['result'][0]['meta']
                
                sec = {}    

                filtered_df = current.filter(pl.col('sec_id').is_in([security_id]))

                if filtered_df.is_empty():
                    counter = max_sec_ik+1
                    max_sec_ik = counter
                else:
                    counter = filtered_df.select(pl.col('sec_ik')).item()

                sec['sec_ik'] = counter

                trading_periods = data.pop('currentTradingPeriod')

                sec['sec_id'] = data['symbol']
                sec['currency'] = data['currency']
                sec['instrument_type'] = data['instrumentType']
                sec['exchange_name'] = data['exchangeName']
                sec['first_trade_date'] = data['firstTradeDate']
                sec['gmtoffset'] = data['gmtoffset']
                sec['timezone'] = data['timezone']
                sec['exchange_timezone_name'] = data['exchangeTimezoneName']

                reg_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['regular']['start']),'%H%H'))
                reg_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['regular']['end']),'%H%H'))

                sec['reg_start_time_utc8'] = reg_start_datetime
                sec['reg_end_time_utc8'] = reg_end_datetime

                pre_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['pre']['start']),'%H%H'))
                pre_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['pre']['end']),'%H%M'))

                sec['pre_start_time_utc8'] = pre_start_datetime
                sec['pre_end_time_utc8'] = pre_end_datetime

                post_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['post']['start']),'%H%M'))
                post_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['post']['end']),'%H%M'))

                sec['post_start_time_utc8'] = post_start_datetime
                sec['post_end_time_utc8'] = post_end_datetime

                sec['cur_ind'] = 1

                metadata.append(sec)

            except Exception as e:
                print(f"Error for {security_id}: {e}")


        __cockroachdb.execute(text('TRUNCATE table "portfoliomonitoring"."public"."securities"'))
        __cockroachdb.commit()
        
        pl.DataFrame(metadata).write_database(
                table_name="securities",
                connection = os.environ["COCKROACH_DB"],
                if_table_exists = 'append',
                engine = 'sqlalchemy'
                )


@task(name='full_refresh')
def refresh_prices(counters):
        
    if counters == []:
        print('Starting Full Refresh')
        __counters = fetch_counters()
        to_refresh = list(__counters)
    
    else:
        print(f"Starting Refresh of [{' '.join(counters)}]")
        to_refresh = counters

    cpu_count = multiprocessing.cpu_count()

    with multiprocessing.Pool(cpu_count) as pool:
        # perform calculations
        results = pool.map(scrape_tasks, to_refresh)
        pool.close()
        pool.join()

        for a in results:
            for logs in a:
                print(logs)


@task(name='incremental_refresh')
def ingest_prices_daily():

    __uniquetickers = get_distinct_tickers()

    for ticker in sorted(__uniquetickers):

        print(f"Updating {ticker}")

        data = get_prices_daily(ticker)
        
        if data  == None:
            continue
        
        quotes = data['indicators']['quote'][0]

        metadata_columns = ['symbol','currency',
        'instrumentType','exchangeName',
        'firstTradeDate','gmtoffset',
            'timezone', 'exchangeTimezoneName']

        metadata =  {key: data['meta'][key] for key in metadata_columns}

        if metadata['symbol'] == 'SGD=X':
            metadata['symbol'] = 'USD/SGD'

        if metadata['symbol'] == 'HKDSGD=X':
            metadata['symbol'] = 'HKD/SGD'

        symbol_ = metadata['symbol']
        currency_ = metadata['currency']
        exchange_ = metadata['exchangeName']
        instr_type_ = metadata['instrumentType']

        print(f"Current no.of.rows : {len(list(__collection.find({'metadata.symbol' : symbol_ ,'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_})))}")

        for i in range(len(data['timestamp'])):
            time_ =  datetime.fromtimestamp(data['timestamp'][i]).replace(hour=0, minute=0, second=0)
            open_ = quotes['open'][i]
            high_ = quotes['high'][i]
            low_ = quotes['low'][i]
            close_ = quotes['close'][i]
            volume_ = quotes['volume'][i]

            __collection.bulk_write(
                [UpdateOne(
                    {'metadata.symbol': symbol_ ,'timestamp':time_, 'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_},
                    {'$set':{ 
                            'open': open_ ,
                            'high': high_,
                            'low': low_,
                            'close': close_,
                            'volume': volume_,
                            "metadata":metadata
                            }
                    },
                    upsert=True
                )]
            )

        print(f"No.of.rows after update : {len(list(__collection.find({'metadata.symbol' : symbol_ ,'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_})))}")


@flow(retries=1, retry_delay_seconds=5, log_prints=True)
def eod_data_feed(name: str = "eod_data_feed"):

   refresh_metadata()
   ingest_prices_daily()

   __cockroachdb.close()
   print('Closed CockroachDB connection')
   __mongoclient.close()
   print('Closed MongoDB connection')
   

if __name__ == "__main__":
    eod_data_feed.serve(
        name="eod_data_feed",
        tags=["pm"],
        parameters={},
        schedule=(CronSchedule(cron="00 01/8 * * 1-5", timezone="Asia/Singapore"))
    )

# class eod_hist_feed_service():

#     def __init__(self,loggerobj):
#         self.__uri = os.getenv("MONGO_DB")
#         self.__mongoclient = MongoClient(self.__uri, server_api=ServerApi('1'))
#         self.__mongodb = self.__mongoclient.prices
#         self.__collection = self.__mongodb["eod_prices"]
#         self.__cockroachdb = create_engine(os.environ["COCKROACH_DB"]).connect()
#         self.__loggerobj = loggerobj



#     def get_distinct_tickers(self):
#         response = self.__cockroachdb.execute(text('SELECT distinct "ticker" FROM "portfoliomonitoring"."public"."transactions"')).fetchall()

#         self.__uniquetickers = [x[0] for x in response]

#         self.__uniquetickers.append('SGD=X')
#         self.__uniquetickers.append('HKDSGD=X')


#     def refresh_metadata(self):

#         self.get_distinct_tickers()

#         metadata = []

#         current = pl.read_database(
#             query='SELECT * FROM "portfoliomonitoring"."public"."securities"',
#             connection =  self.__cockroachdb)

#         if current.is_empty():
#             max_sec_ik = 0
#         else:
#             max_sec_ik = current.select(pl.col('sec_ik')).max().item()
            

#         for security_id in sorted(self.__uniquetickers):
#             url = f"https://query1.finance.yahoo.com/v8/finance/chart/{security_id}?interval=1d&range=1d"
#             headers = {
#                 "Access-Control-Allow-Origin":"https://sg.finance.yahoo.com",
#                 "Content-Type":"application/json;charset=utf-8",
#                 "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"\
#                             " AppleWebKit/537.36 (KHTML, like Gecko)"\
#                             " Chrome/121.0.0.0 Safari/537.36"
#                 }
#             response = requests.get(url,headers=headers)

#             try:
            
#                 data = json.loads(response.text)['chart']['result'][0]['meta']
                
#                 sec = {}    

#                 filtered_df = current.filter(pl.col('sec_id').is_in([security_id]))

#                 if filtered_df.is_empty():
#                     counter = max_sec_ik+1
#                     max_sec_ik = counter
#                 else:
#                     counter = filtered_df.select(pl.col('sec_ik')).item()

#                 sec['sec_ik'] = counter

#                 trading_periods = data.pop('currentTradingPeriod')

#                 sec['sec_id'] = data['symbol']
#                 sec['currency'] = data['currency']
#                 sec['instrument_type'] = data['instrumentType']
#                 sec['exchange_name'] = data['exchangeName']
#                 sec['first_trade_date'] = data['firstTradeDate']
#                 sec['gmtoffset'] = data['gmtoffset']
#                 sec['timezone'] = data['timezone']
#                 sec['exchange_timezone_name'] = data['exchangeTimezoneName']

#                 reg_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['regular']['start']),'%H%H'))
#                 reg_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['regular']['end']),'%H%H'))

#                 sec['reg_start_time_utc8'] = reg_start_datetime
#                 sec['reg_end_time_utc8'] = reg_end_datetime

#                 pre_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['pre']['start']),'%H%H'))
#                 pre_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['pre']['end']),'%H%M'))

#                 sec['pre_start_time_utc8'] = pre_start_datetime
#                 sec['pre_end_time_utc8'] = pre_end_datetime

#                 post_start_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['post']['start']),'%H%M'))
#                 post_end_datetime = int(datetime.strftime(datetime.fromtimestamp(trading_periods['post']['end']),'%H%M'))

#                 sec['post_start_time_utc8'] = post_start_datetime
#                 sec['post_end_time_utc8'] = post_end_datetime

#                 sec['cur_ind'] = 1

#                 metadata.append(sec)

#             except Exception as e:
#                 print(f"Error for {security_id}: {e}")


#         self.__cockroachdb.execute(text('TRUNCATE table "portfoliomonitoring"."public"."securities"'))
#         self.__cockroachdb.commit()
        
#         pl.DataFrame(metadata).write_database(
#                 table_name="securities",
#                 connection = os.environ["COCKROACH_DB"],
#                 if_table_exists = 'append',
#                 engine = 'sqlalchemy'
#                 )


#     def fetch_counters(self):

#         current = pl.read_database(
#             query='SELECT * FROM "portfoliomonitoring"."public"."securities"',
#             connection =  self.__cockroachdb)

#         self.__counters = current.select(pl.col('sec_id')).to_series()


#     def refresh_prices(self,counters):
        
#         if counters == []:
#             print('Starting Full Refresh')
#             self.fetch_counters()
#             to_refresh = list(self.__counters)
        
#         else:
#             print(f"Starting Refresh of [{' '.join(counters)}]")
#             to_refresh = counters

#         cpu_count = multiprocessing.cpu_count()

#         with multiprocessing.Pool(cpu_count) as pool:
#             # perform calculations
#             results = pool.map(scrape_tasks, to_refresh)
#             pool.close()
#             pool.join()

#             for a in results:
#                 for logs in a:
#                     print(logs)


#     def ingest_prices_daily(self):

#         for ticker in sorted(self.__uniquetickers):

#             print(f"Updating {ticker}")

#             data = get_prices_daily(ticker)
            
#             if data  == None:
#                 continue
            
#             quotes = data['indicators']['quote'][0]

#             metadata_columns = ['symbol','currency',
#             'instrumentType','exchangeName',
#             'firstTradeDate','gmtoffset',
#              'timezone', 'exchangeTimezoneName']

#             metadata =  {key: data['meta'][key] for key in metadata_columns}

#             if metadata['symbol'] == 'SGD=X':
#                 metadata['symbol'] = 'USD/SGD'

#             if metadata['symbol'] == 'HKDSGD=X':
#                 metadata['symbol'] = 'HKD/SGD'

#             symbol_ = metadata['symbol']
#             currency_ = metadata['currency']
#             exchange_ = metadata['exchangeName']
#             instr_type_ = metadata['instrumentType']

#             print(f"Current no.of.rows : {len(list(self.__collection.find({'metadata.symbol' : symbol_ ,'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_})))}")

#             for i in range(len(data['timestamp'])):
#                 time_ =  datetime.fromtimestamp(data['timestamp'][i]).replace(hour=0, minute=0, second=0)
#                 open_ = quotes['open'][i]
#                 high_ = quotes['high'][i]
#                 low_ = quotes['low'][i]
#                 close_ = quotes['close'][i]
#                 volume_ = quotes['volume'][i]

#                 self.__collection.bulk_write(
#                     [UpdateOne(
#                         {'metadata.symbol': symbol_ ,'timestamp':time_, 'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_},
#                         {'$set':{ 
#                                 'open': open_ ,
#                                 'high': high_,
#                                 'low': low_,
#                                 'close': close_,
#                                 'volume': volume_,
#                                 "metadata":metadata
#                                 }
#                         },
#                         upsert=True
#                     )]
#                 )

#             print(f"No.of.rows after update : {len(list(self.__collection.find({'metadata.symbol' : symbol_ ,'metadata.currency':currency_, 'metadata.exchangeName':exchange_,'metadata.instrumentType':instr_type_})))}")

# if __name__ == '__main__':   
#     feed_obj = eod_hist_feed_service(loggerobj)
#     #feed_obj.test_connection()
#     feed_obj.refresh_metadata()
#     feed_obj.ingest_prices_daily()
#     #feed_obj.refresh_prices([])