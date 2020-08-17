from pandas import DataFrame
import pandas as pd


class BasicAlgorithm:
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        self.tick_type = tick_type
        self.tick_size = tick_size
        self.order_type = order_type
        self.price = 0
        self.data = pd.DataFrame()
        self.roi = {"100": 0.02}
        self.stoploss = -2

    def set_state(self, state):
        self.data = self.data.append(state, ignore_index=True)

    def update_indicators(self, dataframe: DataFrame) -> DataFrame:
        raise NotImplemented

    def buy_trend(self, dataframe: DataFrame) -> bool:
        raise NotImplemented

    def sell_trend(self, dataframe: DataFrame) -> bool:
        raise NotImplemented

    def action(self, is_trading: bool):
        self.update_indicators(self.data)
        if not is_trading:
            return 'BUY' if self.buy_trend(self.data) else None
        return 'SELL' if self.buy_trend(self.data) else None
