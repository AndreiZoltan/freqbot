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
        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.size)
        if not state.empty:
            self.algorithm.get_state(state)
            action = self.algorithm.action()
            self.act(action)
            self.data = self.data.drop(self.data.loc[self.data["datetime"] <= state.index[-1]].index)

    def get_initial_data(self, pair: str, days: int, override: bool):
        path = os.path.abspath(__file__)
        path = "/".join(path.split("/")[:-1]) + '/data/'
        print(pair, days, path)
        self.data = fm.load_dataset(client=self.client,
                                    pair=pair,
                                    days=days,
                                    path=path,
                                    override=override,
                                    use_swifter=False)

        state = getattr(self.data.bars, self.algorithm.tick_type)(self.algorithm.size)

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

    def act(self, action):
        pass

    def trade(self, pair: str, days: int, override: bool = True):
        """
        This function is used for trading
        :param pair: pair to trade on
        :param days: number of days from which data is collected
        :param override: should data be override ?
        :return: trade function has no return but it saves logs
        """
        self.get_initial_data(pair=pair, days=days, override=override)
        conn_key = self.bm.start_aggtrade_socket(pair, self.handle_message)
        self.bm.start()

    def backtest(self):
        pass

    def dry_trade(self):
        pass
