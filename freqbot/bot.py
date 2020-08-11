from binance.websockets import BinanceSocketManager
from binance.client import Client
from freqml import *
import freqml as fm
import pandas as pd
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
        self.stake_amount = 0
        self.quantity = 0
        self.price = 0
        self.lot_precision = 0
        self.price_precision = 0

    @property
    def is_trading(self):
        if self.order:
            self.order = self.client.get_order(symbol=self.order['symbol'], orderId=self.order['orderId'])
            return False if self.order['status'] == 'FILLED' else True
        return False

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

    def process_message(self, message):
        message["id"] = message.pop("a")
        message["price"] = message.pop("p")
        self.price = float(message["price"])
        message["amount"] = message.pop("q")
        message["timestamp"] = message.pop("T")
        message["datetime"] = pd.to_datetime(message["timestamp"],
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
            self.quantity = "{:0.0{}f}".format(self.stake_amount / self.algorithm.price, self.lot_precision)
            self.request['quantity'] = self.quantity

    def make_market_request(self, action: str):
        self.request['side'] = action
        if action == 'BUY':
            self.quantity = "{:0.0{}f}".format(self.stake_amount / self.price, self.lot_precision)
            self.request['quantity'] = self.quantity

    def act(self, action):
        if action:
            self.make_request(action)
            self.order = self.client.create_order(** self.request)


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
        self.get_initial_data(pair=pair, days=days, override=override)
        conn_key = self.bm.start_aggtrade_socket(pair, self.handle_message)
        self.bm.start()

    def backtest(self):
        pass

    def dry_trade(self):
        pass
