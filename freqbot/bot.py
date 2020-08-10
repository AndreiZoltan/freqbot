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
        self.stake_amount = 0
        self.request = dict()
        self.order = dict()

    @property
    def is_trading(self):
        self.order = self.client.get_order(symbol=self.order['symbol'], orderId=self.order['orderId'])
        return False if self.order['status'] == 'FILLED' else True

    def process_message(self, message):
        message["id"] = message.pop("a")
        message["price"] = message.pop("p")
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
        if not self.data.empty:
            print(self.data.iloc[0, 0], self.data.iloc[-1, 0])
        print(self.data.shape)
        self.data = self.data.append(message, ignore_index=True)
        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.tick_size)
        if not state.empty:
            self.algorithm.get_state(state)
            self.algorithm.is_trading = self.is_trading
            action = self.algorithm.action()
            print(action)
            self.act(action)
            self.data = self.data.drop(self.data.loc[self.data["datetime"] <= state.index[-1]].index)

    def get_initial_data(self, pair: str, days: int, override: bool):
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-1]) + '/data/'
        print(pair, days, path)
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

    def handle_message(self, message):
        message = self.process_message(message)
        self.update(message)

    def create_request(self, pair: str):
        self.request['symbol'] = pair
        self.request['type'] = self.algorithm.order_type
        self.request['newOrderRespType'] = 'FULL'
        self.request['recvWindow'] = 250  # why not?
        if self.algorithm.order_type == 'MARKET':
            self.request['quoteOrderQty'] = self.stake_amount
        else:  # for LIMIT type
            self.request['timeInForce'] = 'GTC'

    def make_request(self, action):
        self.request['side'] = action
        if self.algorithm.order_type == 'LIMIT':
            self.request['price'] = str(self.algorithm.price)
            self.request['quantity'] = self.stake_amount / self.algorithm.price

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
        self.stake_amount = stake_amount
        self.create_request(pair)
        self.get_initial_data(pair=pair, days=days, override=override)
        conn_key = self.bm.start_aggtrade_socket(pair, self.handle_message)
        self.bm.start()

    def backtest(self):
        pass

    def dry_trade(self):
        pass
