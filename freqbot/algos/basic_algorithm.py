from pandas import DataFrame
import pandas as pd


class BasicAlgorithm:
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        self.tick_type = tick_type
        self.tick_size = tick_size
        self.order_type = order_type
        self.price = 0
        self.data = pd.DataFrame()
        self.roi = dict()
        self.stoploss = None

    def set_state(self, state):
        self.data = self.data.append(state, ignore_index=True)
        print("IT IS DATA SHAPE ", self.data.shape)
        print("IT IS APPENDED SHAPE ", state.shape)

    def update_indicators(self, dataframe: DataFrame) -> DataFrame:
        pass

    def buy_trend(self, dataframe: DataFrame) -> bool:
        pass

    def sell_trend(self, dataframe: DataFrame) -> bool:
        pass

    def action(self, is_trading: bool):
        self.update_indicators(self.data)
        if not is_trading:
            return 'BUY' if self.buy_trend(self.data) else None
        return 'SELL' if self.buy_trend(self.data) else None
