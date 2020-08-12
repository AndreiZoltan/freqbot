import freqbot as fb
from binance.websockets import BinanceSocketManager
from binance.client import Client
from freqml import *
import freqml as fm
import pandas as pd
import numpy as np
import time
import os


class Bot:
    def __init__(self, key: str, secret: str, algorithm):
        self.client = Client(key, secret)
        self.bm = BinanceSocketManager(self.client)
        self.algorithm = algorithm
        self.data = pd.DataFrame()
        self.request = dict()
        self.order = None

        # some metadata
        self.meta = fb.OrderMetadata()
        self.stake_amount = None
        self.price = None
        self.is_trading = False
        self.lot_precision = None
        self.price_precision = None

        # some helpers
        self.logs = fb.LogHandler()
        self.make_request = None  # will be defined in set_metadata

    def set_metadata(self, pair, stake_amount):
        def get_precision(string: str):
            precision = 0
            for char in string:
                if char == '1':
                    break
                precision += 1 if char == '0' else 0
            return precision

        self.stake_amount = stake_amount
        info = self.client.get_symbol_info(pair)
        step_size = info['filters'][2]['stepSize']
        self.lot_precision = get_precision(step_size)
        tick_size = info['filters'][0]['tickSize']
        self.price_precision = get_precision(tick_size)
        self.make_request = self.make_limit_request if self.algorithm.order_type == 'LIMIT' else self.make_market_request
        self.meta.set_roi_stoploss(self.algorithm.roi, self.algorithm.stoploss)

    def process_message(self, message):
        message["price"] = message.pop("p")
        self.price = float(message["price"])

        # handling roi or stoploss case
        if self.is_trading:
            if self.meta.roi_stoploss_check(self.price):
                self.make_market_request('SELL')
                self.order = self.client.create_order(** self.request)
                self.meta.handle_order(self.order)
                self.logs.add(self.meta)


        message["id"] = message.pop("a")
        message["amount"] = message.pop("q")
        message["timestamp"] = message.pop("T")
        message["datetime"] = pd.to_datetime(message["timestamp"],  # IS IT A JOKE OR WHAT ???
                                             unit='ms',
                                             utc=True).tz_convert('Europe/Chisinau')
        message["price"] = pd.to_numeric(message["price"])
        message["amount"] = pd.to_numeric(message["amount"])
        message["cost"] = message["price"] * message["amount"]
        message.pop("E", None)
        message.pop("e", None)
        message.pop("s", None)
        del message["f"]
        del message["l"]
        del message["m"]
        del message["M"]
        return message

    def update(self, message):
        self.data = self.data.append(message, ignore_index=True)
        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.tick_size)
        if not state.empty:
            print(state)
            self.algorithm.get_state(state)
            self.algorithm.is_trading = self.is_trading
            action = self.algorithm.action()
            print(action)
            if action:
                self.meta.set_time(action)
                self.act(action)
            self.data = self.data.drop(self.data.loc[self.data["datetime"] <= state.index[-1]].index)

    def get_initial_data(self, pair: str, days: int, override: bool):
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-1]) + '/data/'
        self.data = fm.load_dataset(client=self.client,
                                    pair=pair,
                                    days=days,
                                    path=path,
                                    override=override,
                                    use_swifter=False)

        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.tick_size)

        print("IT IS ME GET INITIAL STATE ", state.shape)
        self.algorithm.get_state(state)

        last_id = self.data.iloc[-1, 0]
        self.data = self.data.drop(self.data.loc[self.data["datetime"] <= state.index[-1]].index)

        # cycle below is just to minimize lag that occurs because of loading dataset
        for i in range(5):
            if not self.data.empty:
                last_id = self.data.iloc[-1, 0]
            agg_trades = self.client.aggregate_trade_iter(symbol=pair, last_id=last_id)
            agg_trades = list(agg_trades)
            messages = [self.process_message(message) for message in agg_trades]
            self.update(messages)

    def handle_order(self, message):
        if message['e'] == 'executionReport':
            if message['S'] == 'BUY':
                self.is_trading = True
                if message['o'] == 'MARKET':
                    self.buy_price = self.logs.get_price(self.order)
            elif message['S'] == 'SELL' and message['X'] == 'FILLED':
                if message['o'] == 'MARKET':
                    self.sell_price = self.logs.get_price(self.order)
                self.is_trading = False

    def handle_message(self, message):
        message = self.process_message(message)
        self.update(message)

    def create_request(self, pair: str):
        self.request['symbol'] = pair
        self.request['type'] = self.algorithm.order_type
        self.request['newOrderRespType'] = 'FULL'
        self.request['recvWindow'] = 250  # why not?
        if self.algorithm.order_type == 'LIMIT':  # for LIMIT type
            self.request['timeInForce'] = 'GTC'

    def make_limit_request(self, action: str):
        self.request['side'] = action

        # may be better to use self.price + n * tick_size instead of self.algorithm.price
        # or IOC with self.price - n * tick_size
        self.request['price'] = "{:0.0{}f}".format(self.algorithm.price, self.price_precision)
        if action == 'BUY':
            self.meta.start_price = self.algorithm.price
            self.meta.quantity = "{:0.0{}f}".format(self.stake_amount / self.algorithm.price, self.lot_precision)
            self.request['quantity'] = self.meta.quantity
        else:
            self.meta.end_price = self.algorithm.price

    def make_market_request(self, action: str):
        self.request['side'] = action
        if action == 'BUY':
            self.meta.quantity = "{:0.0{}f}".format(self.stake_amount / self.price, self.lot_precision)
            self.request['quantity'] = self.meta.quantity

    def act(self, action):
        self.make_request(action)
        self.order = self.client.create_order(** self.request)
        self.meta.handle_order(self.order)
        self.logs.add(self.meta)

    def trade(self, pair: str, days: int, override: bool = True, stake_amount: int = 10):
        """
        This function is used for trading
        :param pair: pair to trade on
        :param days: number of days from which data is collected
        :param override: should data be override ?
        :param stake_amount: amount of one stake
        :return: trade function has no return but it saves logs
        """
        self.set_metadata(pair, stake_amount)
        self.create_request(pair)
        self.get_initial_data(pair, days, override)
        self.bm.start_user_socket(self.handle_order)
        conn_key = self.bm.start_aggtrade_socket(pair, self.handle_message)
        self.bm.start()

    def backtest(self):
        pass

    def dry_trade(self):
        pass
