import pickle
import os.path
from datetime import datetime
import pandas as pd
#from joblib import Memory

from agent.TradingAgent import TradingAgent
from util.order.LimitOrder import LimitOrder
from util.util import log_print

from copy import deepcopy


class MarketReplayAgentUSD(TradingAgent):

    def __init__(self, id, name, type, symbol, date, start_time, end_time,
                 orders_file_path, processed_orders_folder_path,
                 starting_cash, log_orders=False, is_no_impact = False, random_state=None):
        super().__init__(id, name, type, starting_cash=starting_cash, log_orders=log_orders, random_state=random_state)
        self.symbol = symbol
        self.date = date
        self.log_orders = log_orders
        self.executed_trades = dict()
        self.state = 'AWAITING_WAKEUP'

        self.historical_orders = L3OrdersProcessor(self.symbol,
                                                   self.date, start_time, end_time,
                                                   orders_file_path, processed_orders_folder_path)
        self.wakeup_times = self.historical_orders.wakeup_times
        self.is_no_impact = is_no_impact
        self.wakeup_times_restored = []

    def wakeup(self, currentTime):
        super().wakeup(currentTime)
        if not self.mkt_open or not self.mkt_close:
            return
        try:
            #debug
            #if self.historical_orders.orders_dict[currentTime][0]['Order_ID'] < 0:
            #    log_print(f'no impact order {self.historical_orders.orders_dict[currentTime][0]["Order_ID"]}')

            self.placeOrder(currentTime, self.historical_orders.orders_dict[currentTime])

            if self.is_no_impact == False or len(self.wakeup_times_restored) < 1:
                self.setWakeup(self.wakeup_times[0])
                self.wakeup_times.pop(0)
            else:
                if self.wakeup_times[0] <= self.wakeup_times_restored[0]: # == is not expected
                    self.setWakeup(self.wakeup_times[0])
                    self.wakeup_times.pop(0)
                else:
                    self.setWakeup(self.wakeup_times_restored[0])
                    self.wakeup_times_restored.pop(0)

        except IndexError:
            log_print(f"Market Replay Agent submitted all orders - last order @ {currentTime}")

    def receiveMessage(self, currentTime, msg):
        super().receiveMessage(currentTime, msg)
        if msg.body['msg'] == 'ORDER_EXECUTED':
            order = msg.body['order']
            self.executed_trades[currentTime] = [order.fill_price, order.quantity]
            self.last_trade[self.symbol] = order.fill_price

            if self.is_no_impact and order.tag == 2:

                # Here is the closest wake aup time which was setup, and there is one at the top of the queue.
                # We set up our new order time after the one from the queue as we can get it and it is not the past for sure
                # I'd like to schedule new one after closest wake up, but I dont know how to get that time.

                restore_time = max([ self.wakeup_times[0] ]+ self.wakeup_times_restored) + pd.Timedelta(10, unit='ns') 

                while restore_time in self.historical_orders.orders_dict:
                    restore_time +=pd.Timedelta(1, unit='ns')

                new_executed_order = {  'Direction': 'BUY' if order.is_buy_order else 'SELL', 
                                        'Order_ID': -order.order_id, 
                                        'Price': order.limit_price, 
                                        'Size': order.quantity, 
                                        'Type': 'R'}

                self.historical_orders.orders_dict[restore_time] = [new_executed_order]
                self.wakeup_times_restored.append(restore_time)


    def placeOrder(self, currentTime, order):
        if len(order) == 1:
            order = order[0]
            order_id = order['Order_ID']
            existing_order = self.orders.get(order_id)
            if not existing_order and order['Size'] > 0 and order['Type'] == 'R':
                self.placeLimitOrder(self.symbol, order['Size'], order['Direction'] == 'BUY', order['Price'],
                                     order_id=order_id)
            elif existing_order and order['Size'] == 0:
                self.cancelOrder(existing_order)
            elif existing_order and order['Size'] > 0:
                # self.modifyOrder(existing_order, LimitOrder(self.id, currentTime, self.symbol, order['SIZE'],
                #                                             order['BUY_SELL_FLAG'] == 'BUY', order['PRICE'],
                #                                             order_id=order_id))
                self.modifyOrder(existing_order, LimitOrder(self.id, currentTime, self.symbol, order['Size'], 
                                                            order['Direction'] == 'BUY', order['Price'],
                                                            order_id=order_id))
            else:
                None # TODO: check if something is comming here. We should process A and Z types as well
        else:
            for ind_order in order:
                self.placeOrder(currentTime, order=[ind_order])

    def getWakeFrequency(self):
        log_print(f"Market Replay Agent first wake up: {self.historical_orders.first_wakeup}")
        return self.historical_orders.first_wakeup - self.mkt_open


#mem = Memory(cachedir='./cache', verbose=0)


class L3OrdersProcessor:
    DIRECTION = {0: 'BUY', 1: 'SELL'} # 0 - bid, 1-ask

    # Class for reading historical exchange orders stream
    def __init__(self, symbol, date, start_time, end_time, orders_file_path, processed_orders_folder_path):
        self.symbol = symbol
        self.date = date
        self.start_time = start_time
        self.end_time = end_time
        self.orders_file_path = orders_file_path
        self.processed_orders_folder_path = processed_orders_folder_path

        self.orders_dict = self.processOrders(orders_file_path)
        self.wakeup_times = [*self.orders_dict]
        self.first_wakeup = self.wakeup_times[0]
        self.wakeup_times.pop(0)

    def processOrders(self, orders_file_path : str) -> dict:

        # def convertDate(date_str):
        #     try:
        #         return datetime.strptime(date_str, '%Y%m%d%H%M%S.%f')
        #     except ValueError:
        #         return None #convertDate(date_str[:-1])

        #@mem.cache
        def read_processed_orders_file(processed_orders_file):
            with open(processed_orders_file, 'rb') as handle:
                return pickle.load(handle)

        processed_orders_file = f'{self.processed_orders_folder_path}marketreplay_{self.symbol}_{self.date.date()}.pkl'
        if os.path.isfile(processed_orders_file):
            print(f'Processed file exists for {self.symbol} and {self.date.date()}: {processed_orders_file}')
            return read_processed_orders_file(processed_orders_file)
        else:
            print(f'Processed file does not exist for {self.symbol} and {self.date.date()}, processing...')

            orders_df = pd.read_feather(orders_file_path)
            #orders_df['Time'] = orders_df['Time'].dt.tz_localize('Europe/Moscow')
            orders_df = orders_df.loc[(orders_df.Time >= self.start_time) & (orders_df.Time < self.end_time)]
            orders_df['correction'] = orders_df.groupby('Time').cumcount()
            orders_df['Time'] = orders_df['Time'] + orders_df.correction.apply(lambda x: pd.Timedelta(x, unit='ns'))
            orders_df['Direction'] = orders_df['BUY_SELL_FLAG'].astype(int).replace(L3OrdersProcessor.DIRECTION) #TODO:verify
            orders_df.rename(columns={
                'Time':'Timestamp',
                'SIZE':'Size',
                'PRICE':'Price',
                'RECORD_TYPE':'Type',
                'ORDER_ID':'Order_ID'
            }, inplace=True)
            orders_df = orders_df[['Timestamp',  'Order_ID', 'Price', 'Direction', 'Size','Type']]
            orders_df['Size'] = orders_df['Size'] * 1000
            orders_df.set_index('Timestamp', inplace=True)
            log_print(f"Number of Orders: {len(orders_df)}")
            orders_dict = {k: g.to_dict(orient='records') for k, g in orders_df.groupby(level=0)}
            with open(processed_orders_file, 'wb') as handle:
                pickle.dump(orders_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
                print(f'processed file created as {processed_orders_file}')
            return orders_dict
