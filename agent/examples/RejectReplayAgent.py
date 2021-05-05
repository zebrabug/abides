import pickle
import os.path
from datetime import datetime
import pandas as pd
# from joblib import Memory

from agent.TradingAgent import TradingAgent
from util.order.LimitOrder import LimitOrder
from util.util import log_print


class RejectReplayAgent(TradingAgent):

    def __init__(self, id, name, type, symbol, date, start_time, end_time,
                 orders_file_path, processed_orders_folder_path,
                 starting_cash, log_orders=False, random_state=None):
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

    def wakeup(self, currentTime):
        super().wakeup(currentTime)
        if not self.mkt_open or not self.mkt_close:
            return
        try:
            self.placeOrder(currentTime, self.historical_orders.orders_dict[currentTime])
            self.setWakeup(self.wakeup_times[0])
            self.wakeup_times.pop(0)
        except IndexError:
            log_print(f"Reject Replay Agent submitted all orders - last order @ {currentTime}")

    def receiveMessage(self, currentTime, msg):
        super().receiveMessage(currentTime, msg)
        if msg.body['msg'] == 'ORDER_EXECUTED':
            order = msg.body['order']
            self.executed_trades[currentTime] = [order.fill_price, order.quantity]
            self.last_trade[self.symbol] = order.fill_price

    def placeOrder(self, currentTime, order):
        if len(order) == 1:
            order = order[0]
            order_id = order['Order_ID']
            existing_order = self.orders.get(order_id)
            if not existing_order and order['Size'] > 0 and order['Type'] == 'REJECT':
                if self.symbol not in self.holdings:
                    self.holdings[self.symbol] = 0
                if order['Direction'] == 'BUY':
                    self.holdings['CASH'] -= (order['Size'] * order['Price'])
                    self.holdings[self.symbol] += order['Size']
                    self.logEvent('HOLDINGS_UPDATED', self.holdings)
                elif order['Direction'] == 'SELL':
                    self.holdings['CASH'] += (order['Size'] * order['Price'])
                    self.holdings[self.symbol] -= order['Size']
                    self.logEvent('HOLDINGS_UPDATED', self.holdings)
                else:
                    None

            elif not existing_order and order['Size'] > 0 and order['Type'] == 'COMPENSATION':
                self.placeMarketOrder( self.symbol, order['Size'],order['Direction'] == 'BUY', order_id=order_id)

            elif existing_order and order['Size'] == 0:
                self.cancelOrder(existing_order) # we don't expect such things
            elif existing_order and order['Size'] > 0:
                None # we don't expect this
                # self.modifyOrder(existing_order, LimitOrder(self.id, currentTime, self.symbol, order['Size'],
                #                                             order['Direction'] == 'BUY', order['Price'],
                #                                             order_id=order_id))
            else:
                None  # TODO: check if something is comming here. We should process A and Z types as well
        else:
            for ind_order in order:
                self.placeOrder(currentTime, order=[ind_order])

    def getWakeFrequency(self):
        log_print(f"Reject Replay Agent first wake up: {self.historical_orders.first_wakeup}")
        return self.historical_orders.first_wakeup - self.mkt_open


# mem = Memory(cachedir='./cache', verbose=0)


class L3OrdersProcessor:
    DIRECTION = {0: 'BUY', 1: 'SELL'}  # 0 - bid, 1-ask

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

    def processOrders(self, orders_file_path: str) -> dict:

        # def convertDate(date_str):
        #     try:
        #         return datetime.strptime(date_str, '%Y%m%d%H%M%S.%f')
        #     except ValueError:
        #         return None #convertDate(date_str[:-1])

        # @mem.cache
        def read_processed_orders_file(processed_orders_file):
            with open(processed_orders_file, 'rb') as handle:
                return pickle.load(handle)

        processed_orders_file = f'{self.processed_orders_folder_path}rejectreplay_{self.symbol}_{self.date.date()}.pkl'
        if os.path.isfile(processed_orders_file):
            print(f'Processed file exists for {self.symbol} and {self.date.date()}: {processed_orders_file}')
            return read_processed_orders_file(processed_orders_file)
        else:
            print(f'Processed file does not exist for {self.symbol} and {self.date.date()}, processing...')

            prep_deals = pd.read_pickle(orders_file_path)
            prep_deals = prep_deals.loc[(prep_deals.Moment >= self.start_time) & (prep_deals.Moment <= self.end_time)]
            prep_deals['Order_ID'] = prep_deals.index
            prep_deals['Deals_or_reject'] = prep_deals['Type']
            prep_deals['Type'] = 'R'
            prep_deals['Direction'] = prep_deals['BookSide']
            prep_deals['correction'] = prep_deals.groupby('Moment').cumcount()
            prep_deals['Moment'] = prep_deals['Moment'] + prep_deals.correction.apply(
                lambda x: pd.Timedelta(x, unit='ns'))
            prep_deals.rename(columns={
                'Moment': 'Timestamp',
                'BaseQty': 'Size',
                'ReferenceClientPrice': 'Price',
                'ORDER_ID': 'Order_ID',
                'RECORD_TYPE': 'Type'
            }, inplace=True)
            rejects = prep_deals.loc[
                prep_deals.Deals_or_reject == 'Reject',
                ['Timestamp', 'Order_ID', 'Price', 'Direction', 'Size', 'Type']]
            rejects['Type'] = 'REJECT'
            rejects['Size'] = rejects['Size'].abs()
            compensation = rejects.copy()
            compensation['Timestamp'] = compensation['Timestamp'] + pd.Timedelta(2, unit='s')
            compensation['Direction'] = compensation['Direction'].replace({'BUY': 'SELL', 'SELL': 'BUY'})
            compensation['Type'] = 'COMPENSATION'
            #compensation['SEQ_NUM'] = 0
            compensation['Order_ID'] = compensation['Order_ID'] + rejects['Order_ID'].max()
            orders_df = pd.concat([rejects, compensation]).sort_values(['Timestamp', 'Order_ID'],
                                                                       ignore_index=True)
            orders_df['Order_ID'] = orders_df['Order_ID'].astype(int)
            orders_df.set_index('Timestamp', inplace=True)
            #DEBUG
            #l = [123, 123 + rejects['Order_ID'].max(), 130, 130 + rejects['Order_ID'].max()]  # get a couple examples
            #orders_df = orders_df[orders_df['Order_ID'].apply(lambda x: x in l)]
            #DEBUG
            log_print(f"Number of Orders: {len(orders_df)}")
            orders_dict = {k: g.to_dict(orient='records') for k, g in orders_df.groupby(level=0)}
            with open(processed_orders_file, 'wb') as handle:
                pickle.dump(orders_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
                print(f'processed file created as {processed_orders_file}')
            return orders_dict
