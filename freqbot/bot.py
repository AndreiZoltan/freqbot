import freqbot as fb
from binance.websockets import BinanceSocketManager
from binance.client import Client
from freqml import *
import freqml as fm
import pandas as pd
import numpy as np
import time
import os


class OrderMetadata:
    def __init__(self):
        self.quantity = None
        self.start_price = None
        self.end_price = None
        self.pair = None
        self.start_time = None
        self.end_time = None
        self.roi = dict()
        self.stoploss = None
        self.sell_cause = None
        # self.is_trading = False  # may be is unnecessarily

    def set_roi_stoploss(self, roi: dict, stoploss: float):
        self.roi = {float(key): value for key, value in roi.items()}
        self.stoploss = stoploss

    def set_time(self, action: str):
        if action == 'BUY':
            self.start_time = time.perf_counter()
        else:
            self.end_time = time.perf_counter()

    def roi_stoploss_check(self, price: float):
        diff = time.perf_counter() - self.start_time
        keys = np.array(list(self.roi.keys()))
        key = np.min(keys[keys - diff > 0])
        profit_price = self.start_price * (1 + self.roi[key])
        if price > profit_price:
            self.set_time('SELL')
            self.sell_cause = 'ROI'
            return True
        if price < self.start_price * (1 + self.stoploss):
            self.set_time('SELL')
            self.sell_cause = 'STOPLOSS'
            return True
        return False

    @staticmethod
    def get_price(order: dict):
        cost = 0
        quantity = 0
        for fill in order['fills']:
            cost += float(fill['price']) * float(fill['qty'])
            quantity += float(fill['qty'])
        return cost / quantity

    def handle_order(self, order: dict):
        self.pair = order['symbol']
        if order['type'] == 'MARKET':
            if order['side'] == 'BUY':
                self.start_price = self.get_price(order)
            else:
                self.end_price = self.get_price(order)
        if order['side'] == 'SELL':
            self.sell_cause = 'SELL SIGNAL'

    def flush(self):
        self.quantity = None
        self.start_price = None
        self.end_price = None
        self.pair = None
        self.start_time = None
        self.end_time = None
        self.sell_cause = None


class Bot:
    def __init__(self, key: str, secret: str, algorithm):
        self.client = Client(key, secret)
        self.bm = BinanceSocketManager(self.client)
        self.algorithm = algorithm
        self.data = pd.DataFrame()
        self.request = dict()
        self.order = None

        # some metadata
        self.meta = OrderMetadata()
        self.stake_amount = None
        self.price = None
        self.is_trading = False
        self.lot_precision = None
        self.price_precision = None

        # some helpers
        self.data_handler = fb.DataHandler()

    def create_request(self, pair: str):
        self.request['symbol'] = pair
        self.request['type'] = self.algorithm.order_type
        self.request['newOrderRespType'] = 'FULL'
        self.request['recvWindow'] = 250  # why not?
        if self.algorithm.order_type == 'LIMIT':  # for LIMIT type
            self.request['timeInForce'] = 'GTC'

    def set_metadata(self, pair: str, stake_amount: int):
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
        self.meta.set_roi_stoploss(self.algorithm.roi, self.algorithm.stoploss)
        self.create_request(pair)

    def process_message(self, message):
        message["price"] = message.pop("p")
        self.price = float(message["price"])

        # handling roi or stoploss case
        if self.is_trading:
            if self.meta.roi_stoploss_check(self.price):
                self.act('SELL', 'MARKET')


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
            self.algorithm.get_state(state)
            self.algorithm.is_trading = self.is_trading
            action = self.algorithm.action()
            if action:
                self.act(action, self.algorithm.order_type)
            self.data = self.data.drop(self.data.loc[self.data["datetime"] <= state.index[-1]].index)

    def get_historical_data(self, pair: str, days: int, override: bool):
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-2]) + '/data/'
        self.data = fm.load_dataset(client=self.client,
                                    pair=pair,
                                    days=days,
                                    path=path,
                                    override=override,
                                    use_swifter=False)

        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.tick_size)

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
                self.meta.set_time('BUY')
                self.is_trading = True
            elif message['S'] == 'SELL' and message['X'] == 'FILLED':
                self.meta.set_time('SELL')
                self.data_handler.add(self.meta)
                self.meta.flush()
                self.is_trading = False

    def handle_message(self, message):
        message = self.process_message(message)
        self.update(message)

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

    def act(self, action: str, type_order: str):
        if type_order == 'MARKET':
            self.make_market_request(action)
        else:
            self.make_limit_request(action)
        self.order = self.client.create_order(** self.request)
        self.meta.handle_order(self.order)

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
        self.get_historical_data(pair, days, override)
        self.bm.start_user_socket(self.handle_order)
        conn_key = self.bm.start_aggtrade_socket(pair, self.handle_message)
        self.bm.start()

    def backtest(self):
        pass

    def dry_trade(self):
        pass
