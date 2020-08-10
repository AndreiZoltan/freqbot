from pandas import DataFrame
import pandas as pd


class BasicAlgorithm:
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        self.tick_type = tick_type
        self.tick_size = tick_size
        self.order_type = order_type
        self.price = 0
        self.is_trading = False
        self.data = pd.DataFrame()

    def get_state(self, state):
        self.data = self.data.append(state, ignore_index=True)
        print("IT IS DATA SHAPE ", self.data.shape)
        print("IT IS APPENDED SHAPE ", state.shape)

    def update_indicators(self, dataframe: DataFrame) -> DataFrame:
        pass

    def buy_trend(self, dataframe: DataFrame) -> bool:
        pass

    def sell_trend(self, dataframe: DataFrame) -> bool:
        pass

    def action(self):
        self.update_indicators(self.data)
        if self.buy_trend(self.data) and not self.is_trading:
            return 'BUY'
        if self.sell_trend(self.data) and self.is_trading:
            return 'SELL'
        return None
