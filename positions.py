from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sqlalchemy import create_engine, text
import polars as pl
import datetime
import pytz
import json
from datetime import datetime
import os


class positions_service():

    def __init__(self):
        self.__transdbconn = create_engine(os.environ["COCKROACH_DB"]).connect()
        self.__pricescollection = MongoClient(os.getenv("MONGO_DB"), server_api=ServerApi('1')).prices["eod_prices"]

    def fetch_transactions(self):
        self.__transactions = pl.read_database(
            query='SELECT * FROM "portfoliomonitoring"."public"."transactions"',
            connection = self.__transdbconn )

        print(f'Transactions : {self.__transactions.shape}')


    def calculate_coi(self, method_='WEIGHTED_AVERAGE'):

        main = pl.DataFrame()

        def coi(df, method):
            
            '''
            Calculate Cost of Investment using FIFO/LIFO/WEIGHTED_AVERAGE method
            
            '''

            if method == 'FIFO':

                _trades = []

                output_series = []

                for i in range(df.shape[0]):
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']
                    
                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees])
                        val = (_quantity * _price) + _fees
                        output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
                    elif _type == 'Sell':
                        remaining_qty = _quantity
                        while remaining_qty > 0:
                            #print(1, remaining_qty,_trades)
                            earliest_trade = _trades.pop(0)
                            earliest_trade_qty = earliest_trade[0]
                            if remaining_qty > earliest_trade_qty:
                                remaining_qty -= earliest_trade_qty
                                #print(2,remaining_qty ,_trades)
                            else:
                                earliest_trade[0] = earliest_trade_qty - remaining_qty
                                if earliest_trade[0] == 0:
                                    #print(3,remaining_qty,_trades)
                                    output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
                                    break
                                else:
                                    _trades.insert(0,earliest_trade)
                                    #print(4,remaining_qty,_trades)
                                    output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
                                    break
                    else:
                        output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
                return output_series

            elif method == 'LIFO':

                _trades = []
                output_series = []

                for i in range(df.shape[0]):
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']

                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees])
                        val = (_quantity * _price) + _fees
                        output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
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
                                    remaining_cost = sum([(qty*price)+ fees for qty,price, fees in _trades])
                                    output_series.append(remaining_cost)
                                    break
                                else:
                                    _trades.append(latest_trade)
                                    #print(4,remaining_qty,_trades)
                                    remaining_cost = sum([(qty*price)+ fees for qty,price, fees in _trades])
                                    output_series.append(remaining_cost)
                                    break
                    else:
                        remaining_cost = sum([(qty*price)+ fees for qty,price, fees in _trades])
                        output_series.append(remaining_cost)

                return output_series

            elif method == 'WEIGHTED_AVERAGE':

                _trades = []
                output_series = []

                for i in range(df.shape[0]):
                    
                    _type = df[i,'type']
                    _quantity = df[i,'quantity']
                    _price = df[i,'price']
                    _fees = df[i,'fees']

                    if _type == 'Buy':
                        _trades.append([_quantity,_price,_fees])
                        val = (_quantity * _price) + _fees
                        output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))
                    elif _type == 'Sell':
                        sum_quantity = sum([x[0] for x in _trades])
                        weights = [x[0]/sum_quantity for x in _trades]

                        for i in range(len(weights)):
                            _trades[i][0] -= (weights[i]*_quantity)

                        _trades = [x for x in _trades if x[0] != 0]

                        output_series.append(sum([(qty*price)+ fees for qty,price, fees in _trades]))       

                    else:
                        remaining_cost = sum([(qty*price)+ fees for qty,price, fees in _trades])
                        output_series.append(remaining_cost)

                return output_series


        for sec in self.__transactions.select('ticker').unique().to_series().to_list():
            curr = self.__transactions.filter(pl.col('ticker')==sec)
            series_output = coi(curr, method_)
            curr = curr.with_columns(pl.Series(name="cum_cost" , values=series_output))
            
            main = pl.concat([main,curr],how='vertical').sort('date')

        self.__transactions = main


    def parse_transactions(self):

        self.calculate_coi()

        def sign(row: pl.col) -> pl.Int64:
            if row == 'Sell':
                return -1
            elif row == 'Buy':
                return 1
            else:
                return 0
        
        transactions = self.__transactions.with_columns(pl.col('type').map_elements(sign).alias('sign'))
            
        transactions = transactions.with_columns(pl.struct(['sign','quantity']).map_elements(lambda row: row['quantity'] * row['sign']).alias('quantity_signed'))

        transactions = transactions.with_columns(pl.cum_sum('quantity_signed').over('ticker').alias('cum_quantity'))
            
        transactions = transactions.with_columns(pl.struct(['quantity','price']).map_elements(lambda row: row['quantity']*row['price']).alias('clean_value_qc'))

        transactions = transactions.with_columns(pl.struct(['type','clean_value_qc','fees']).map_elements(lambda row: row['clean_value_qc'] + row['fees']).alias('dirty_value_qc'))

        transactions = transactions.with_columns(pl.col('cum_quantity').shift(1).over('ticker').alias('lagged_cum_quantity'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Buy' else 0.0).alias('investment_qc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Sell' else 0.0).alias('divestment_qc'))

        transactions = transactions.with_columns(pl.struct(['type','dirty_value_qc']).map_elements(lambda row: row['dirty_value_qc'] if row['type'] == 'Div' else 0.0).alias('income_qc'))

        def cash_flow(row: pl.struct) -> pl.Float64:
            if row['type'] == 'Buy':
                return -1 * (row['clean_value_qc'] + row['fees'])
            elif row['type'] == 'Sell':
                return row['clean_value_qc'] - row['fees']
            else:
                return row['clean_value_qc'] + row['fees']

        transactions = transactions.with_columns(pl.struct(['type','clean_value_qc','fees']).map_elements(cash_flow).alias('cashflow'))

        self.__transactions = transactions

        print(self.__transactions.filter(pl.col('ticker')=='G13.SI'))


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

        positions = self.__transactions#.filter(pl.col('ticker')=='G13.SI')
        
        # Run prices function
        self.get_latest_prices()

        positions = positions.group_by(['security_name','ticker','currency']).agg([pl.sum('quantity_signed'),pl.last('cum_cost'),pl.sum('cashflow'),pl.sum('investment_qc'),pl.sum('divestment_qc'),pl.sum('income_qc'),pl.sum('fees')])

        # Join in prices data
        positions = positions.join(self.__prices, 
                        how='left',
                        left_on=['ticker','currency'],
                        right_on=['symbol','currency']
        )

        positions = positions.with_columns(pl.col("latest_px").fill_null(0))

        
        positions = positions.with_columns(pl.struct(['quantity_signed','latest_px']).map_elements(lambda row: row['quantity_signed']*row['latest_px']).alias('market_value_qc'))

        positions = positions.with_columns(pl.struct(['cashflow','market_value_qc']).map_elements(lambda row: row['cashflow']+row['market_value_qc']).alias('total_pnl_qc'))

        positions = positions.with_columns(pl.struct(['cum_cost','market_value_qc']).map_elements(lambda row: row['market_value_qc']-row['cum_cost']).alias('unrealised_pnl_qc'))

        positions = positions.with_columns(pl.struct(['total_pnl_qc','unrealised_pnl_qc','income_qc']).map_elements(lambda row: row['total_pnl_qc']-row['unrealised_pnl_qc']-row['income_qc']).alias('realised_pnl_qc'))

        self.__positions = positions


pos_obj = positions_service()
pos_obj.fetch_transactions()
pos_obj.parse_transactions()
pos_obj.create_positions()
