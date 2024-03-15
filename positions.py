from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sqlalchemy import create_engine, text
import polars as pl
import datetime
import pytz
import json
from datetime import datetime,  timedelta, date
import os
from collections import deque

class positions_service():

    def __init__(self):
        self.__transdbconn = create_engine(os.environ["COCKROACH_DB"]).connect()
        self.__pricescollection = MongoClient(os.getenv("MONGO_DB"), server_api=ServerApi('1')).prices["eod_prices"]


    def fetch_transactions(self):
        self.__transactions = pl.read_database(
            query='SELECT * FROM "portfoliomonitoring"."public"."transactions"',
            connection = self.__transdbconn )


    def calculate_coi(self, method_='WEIGHTED_AVERAGE'):

        main = pl.DataFrame()

        def coi(df, method):
            
            '''
            Calculate Cost of Investment using FIFO/LIFO/WEIGHTED_AVERAGE method
            
            '''

            if method == 'FIFO':

                _trades = deque([])

                qc_output_series = deque([])
                gc_output_series = deque([])

                for i in range(df.shape[0]):
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']
                    _fx = df[i,'to_sgd']

                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees,_fx])
                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))
                    elif _type == 'Sell':
                        remaining_qty = _quantity
                        while remaining_qty > 0:
                            #print(1, remaining_qty,_trades)
                            earliest_trade = _trades.popleft()
                            earliest_trade_qty = earliest_trade[0]
                            if remaining_qty > earliest_trade_qty:
                                remaining_qty -= earliest_trade_qty
                                #print(2,remaining_qty ,_trades)
                            else:
                                earliest_trade[0] = earliest_trade_qty - remaining_qty
                                if earliest_trade[0] == 0:
                                    #print(3,remaining_qty,_trades)
                                    qc_output_series.append(sum([(qty*price)+ fees for qty,price, fees,_fx in _trades]))
                                    gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty,price, fees,_fx in _trades]))
                                    break
                                else:
                                    _trades.insert(0,earliest_trade)
                                    #print(4,remaining_qty,_trades)
                                    qc_output_series.append(sum([(qty*price)+ fees for qty,price, fees,_fx in _trades]))
                                    gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty,price, fees,_fx in _trades]))
                                    break
                    else:
                        qc_output_series.append(sum([(qty*price)+ fees for qty,price, fees,_fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty,price, fees,_fx in _trades]))
                return qc_output_series,gc_output_series

            elif method == 'LIFO':

                _trades = deque([])
                qc_output_series = deque([])
                gc_output_series = deque([])

                for i in range(df.shape[0]):
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']
                    _fx = df[i,'to_sgd']

                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees,_fx])
                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))
                    elif _type == 'Sell':
                        remaining_qty = _quantity
                        while remaining_qty > 0:
                            #print(1, remaining_qty,_trades)
                            latest_trade = _trades.pop()
                            latest_trade_qty = latest_trade[0]
                            if remaining_qty > latest_trade_qty:
                                remaining_qty -= latest_trade_qty
                                #print(2,remaining_qty ,_trades)
                            else:
                                latest_trade[0] = latest_trade_qty - remaining_qty
                                if latest_trade[0] == 0:
                                    #print(3,remaining_qty,_trades)
                                    qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                                    gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))
                                    break
                                else:
                                    _trades.append(latest_trade)
                                    #print(4,remaining_qty,_trades)
                                    qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                                    gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))
                                    break
                    else:
                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))

                return qc_output_series,gc_output_series

            elif method == 'WEIGHTED_AVERAGE':

                _trades = deque([])
                qc_output_series = deque([])
                gc_output_series = deque([])

                for i in range(df.shape[0]):
                    
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']
                    _fx = df[i,'to_sgd']

                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees,_fx])
                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))
                    elif _type == 'Sell':
                        sum_quantity = sum([x[0] for x in _trades])
                        weights = [x[0]/sum_quantity for x in _trades]

                        for i in range(len(weights)):
                            _trades[i][0] -= (weights[i]*_quantity)

                        _trades = [x for x in _trades if x[0] != 0]

                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))     

                    else:
                        qc_output_series.append(sum([(qty*price)+ fees for qty, price, fees, _fx in _trades]))
                        gc_output_series.append(sum([_fx*((qty*price)+ fees) for qty, price, fees, _fx in _trades]))

                return qc_output_series,gc_output_series


        for sec in self.__transactions.select('ticker').unique().to_series().to_list():
            curr = self.__transactions.filter(pl.col('ticker')==sec)
            qc_series_output, gc_series_output = coi(curr, method_)
            curr = curr.with_columns(pl.Series(name="cum_cost_qc" , values=qc_series_output))
            curr = curr.with_columns(pl.Series(name="cum_cost_gc" , values=gc_series_output))
            
            main = pl.concat([main,curr],how='vertical').sort('date')

        self.__transactions = main


    def get_fx(self):

        main = pl.DataFrame()

        earliest_date = self.__transactions.select('date').min().item()
        latest_date = datetime.now() #self.__transactions.select('date').max().item()

        '''
        Function to fetch fx_rates
        '''

        currencies = ['USD/SGD','HKD/SGD']

        date_range = pl.datetime_ranges(earliest_date,latest_date,'1d',eager=True).to_list()[0]
        date_ranges = [date_range for cur in currencies] 
        full_dates = pl.DataFrame({"date":date_ranges,"currency":currencies}).explode('date')


        results = list(self.__pricescollection.find({'metadata.symbol':{'$in':currencies},'timestamp':{'$gte':earliest_date,'$lte':latest_date}}))

        fx = pl.DataFrame([{
            'timestamp':item['timestamp'],
            'symbol':item['metadata']['symbol'],
            'close':item['close']
            }
            for item in results])

        fx = full_dates.join(fx, 
                how='left',
                left_on=['date','currency'],
                right_on=['timestamp','symbol']
                )

        fx = fx.with_columns(pl.col('currency').map_elements(lambda row:row.split('/')[0]).alias('price_currency'))
        fx = fx.drop(['currency'])
        fx = fx.rename({'close':'to_sgd'})

        for cur in fx.select('price_currency').unique().rows():
            curr = fx.filter(pl.col('price_currency')==cur).sort('date')
            curr = curr.with_columns(pl.col('to_sgd').fill_null(strategy='forward'))
            main = pl.concat([main,curr],how='vertical').sort('date')
            
        self.__fx = main


    def parse_transactions(self):

        self.get_fx()

        self.__transactions = self.__transactions.join(self.__fx, 
                how='left',
                left_on=['date','currency'],
                right_on=['date','price_currency']
                )

        def fill_sgd(row):
            if row['currency'] == 'SGD':
                return 1.0
            else:
                return row['to_sgd']

        self.__transactions = self.__transactions.with_columns(pl.struct(['currency','to_sgd']).map_elements(fill_sgd).alias('to_sgd'))

        self.calculate_coi()

        def sign(row: pl.col) -> pl.Int64:
            if row == 'Sell':
                return -1
            elif row == 'Buy':
                return 1
            else:
                return 0

        transactions = self.__transactions.with_columns(pl.col('type').map_elements(sign).alias('sign'))

        transactions = transactions.with_columns(pl.struct(['fees','to_sgd']).map_elements(lambda row: row['fees'] * row['to_sgd']).alias('fees_gc'))
            
        transactions = transactions.with_columns(pl.struct(['sign','quantity']).map_elements(lambda row: row['quantity'] * row['sign']).alias('quantity_signed'))

        transactions = transactions.with_columns(pl.cum_sum('quantity_signed').over('ticker').alias('cum_quantity'))
            
        transactions = transactions.with_columns(pl.struct(['quantity','price']).map_elements(lambda row: row['quantity']*row['price']).alias('clean_value_qc'))

        transactions = transactions.with_columns(pl.struct(['clean_value_qc','to_sgd']).map_elements(lambda row: row['clean_value_qc']*row['to_sgd']).alias('clean_value_gc'))

        transactions = transactions.with_columns(pl.struct(['clean_value_qc','fees']).map_elements(lambda row: row['clean_value_qc'] + row['fees']).alias('dirty_value_qc'))

        transactions = transactions.with_columns(pl.struct(['clean_value_gc','fees_gc']).map_elements(lambda row: (row['clean_value_gc']+(row['fees_gc']))).alias('dirty_value_gc'))

        transactions = transactions.with_columns(pl.col('cum_quantity').shift(1).over('ticker').alias('lagged_cum_quantity'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Buy' else 0.0).alias('investment_qc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_gc']).map_elements(lambda row: row['dirty_value_gc'] if row['type'] == 'Buy' else 0.0).alias('investment_gc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Sell' else 0.0).alias('divestment_qc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_gc']).map_elements(lambda row: row['dirty_value_gc'] if row['type'] == 'Sell' else 0.0).alias('divestment_gc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Div' else 0.0).alias('income_qc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_gc']).map_elements(lambda row: row['dirty_value_gc'] if row['type'] == 'Div' else 0.0).alias('income_gc'))

        def cash_flow_qc(row: pl.struct) -> pl.Float64:
            if row['type'] == 'Buy':
                return -1 * (row['clean_value_qc'] + row['fees'])
            elif row['type'] == 'Sell':
                return row['clean_value_qc'] - row['fees']
            else:
                return row['clean_value_qc'] + row['fees']
            
        def cash_flow_gc(row: pl.struct) -> pl.Float64:
            if row['type'] == 'Buy':
                return -1 * (row['clean_value_gc'] + row['fees_gc'])
            elif row['type'] == 'Sell':
                return row['clean_value_gc'] - row['fees_gc']
            else:
                return row['clean_value_gc'] + row['fees_gc']

        transactions = transactions.with_columns(pl.struct(['type','clean_value_qc','fees']).map_elements(cash_flow_qc).alias('cashflow_qc'))

        transactions = transactions.with_columns(pl.struct(['type','clean_value_gc','fees_gc']).map_elements(cash_flow_gc).alias('cashflow_gc'))

        self.__transactions = transactions
        

    def get_latest_prices(self):
        '''
        Function to fetch latest close from prices database
        '''
        pipeline = [
            {
                '$group':{'_id':{'symbol':"$metadata.symbol",'currency':"$metadata.currency"}, "maxdate": {"$max":"$timestamp"},"latest_close": {"$last":"$close"}},
            }
        ]

        results = list(self.__pricescollection.aggregate(pipeline))

        self.__prices = pl.DataFrame([{
            'symbol':item['_id']['symbol'],
            'currency':item['_id']['currency'],
            'latest_px_date':item['maxdate'],
            'latest_px':item['latest_close']
            } for item in results])


    def create_positions(self):

        positions = self.__transactions
        
        # Run prices function
        self.get_latest_prices()

        positions = positions.group_by(['security_name','ticker','currency']).agg([
            pl.sum('quantity_signed'),pl.last('cum_cost_qc'),pl.last('cum_cost_gc'),
            pl.sum('cashflow_qc'),pl.sum('investment_qc'),pl.sum('divestment_qc'),pl.sum('income_qc'),pl.sum('fees'),
            pl.sum('cashflow_gc'),pl.sum('investment_gc'),pl.sum('divestment_gc'),pl.sum('income_gc'),pl.sum('fees_gc')  
            ])

        latest_fx = self.__fx.group_by('price_currency').last().drop('date')

        # Join in prices data
        positions = positions.join(self.__prices, 
                        how='left',
                        left_on=['ticker','currency'],
                        right_on=['symbol','currency']
        )

        # Join in fx data
        positions = positions.join(latest_fx , 
                how='left',
                left_on=['currency'],
                right_on=['price_currency']
        )
        
        def fill_sgd(row):
            if row['currency'] == 'SGD':
                return 1.0
            else:
                return row['to_sgd']

        positions = positions.with_columns(pl.struct(['currency','to_sgd']).map_elements(fill_sgd).alias('to_sgd'))

        positions = positions.with_columns(pl.col("latest_px").fill_null(0))
        
        positions = positions.with_columns(pl.struct(['quantity_signed','latest_px']).map_elements(lambda row: row['quantity_signed']*row['latest_px']).alias('market_value_qc'))

        positions = positions.with_columns(pl.struct(['market_value_qc','to_sgd']).map_elements(lambda row: row['market_value_qc']*row['to_sgd']).alias('market_value_gc'))

        positions = positions.with_columns(pl.struct(['cashflow_qc','market_value_qc']).map_elements(lambda row: row['cashflow_qc']+row['market_value_qc']).alias('total_pnl_qc'))

        positions = positions.with_columns(pl.struct(['cashflow_gc','market_value_gc']).map_elements(lambda row: row['cashflow_gc']+row['market_value_gc']).alias('total_pnl_gc'))

        positions = positions.with_columns(pl.struct(['cum_cost_qc','market_value_qc']).map_elements(lambda row: row['market_value_qc']-row['cum_cost_qc']).alias('unrealised_pnl_qc'))

        positions = positions.with_columns(pl.struct(['cum_cost_gc','market_value_gc']).map_elements(lambda row: row['market_value_gc']-row['cum_cost_gc']).alias('unrealised_pnl_gc'))

        positions = positions.with_columns(pl.struct(['total_pnl_qc','unrealised_pnl_qc','income_qc']).map_elements(lambda row: row['total_pnl_qc']-row['unrealised_pnl_qc']-row['income_qc']).alias('realised_pnl_qc'))

        positions = positions.with_columns(pl.struct(['total_pnl_gc','unrealised_pnl_gc','income_gc']).map_elements(lambda row: row['total_pnl_gc']-row['unrealised_pnl_gc']-row['income_gc']).alias('realised_pnl_gc'))

        positions = positions.with_columns(valuation_date = pl.lit(f'{datetime.now().date()}'))

        self.__positions = positions
        with pl.Config(tbl_rows=50,tbl_cols=20):
            print(self.__positions.filter(pl.col('quantity_signed')!=0.0))


pos_obj = positions_service()
pos_obj.fetch_transactions()
pos_obj.parse_transactions()
pos_obj.create_positions()
