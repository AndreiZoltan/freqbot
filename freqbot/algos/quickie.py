from freqbot.algos import BasicAlgorithm
from pandas import DataFrame
import pandas as pd
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib


class Quickie(BasicAlgorithm):
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        super(Quickie, self).__init__(tick_type, tick_size, order_type)
        self.roi = {
            "100": 0.01,
            "30": 0.03,
            "15": 0.06,
            "10": 0.15,
        }

    def update_indicators(self, dataframe: DataFrame) -> DataFrame:
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['macdhist'] = macd['macdhist']

        dataframe['tema'] = ta.TEMA(dataframe, timeperiod=9)
        dataframe['sma_200'] = ta.SMA(dataframe, timeperiod=200)
        dataframe['sma_50'] = ta.SMA(dataframe, timeperiod=200)

        dataframe['adx'] = ta.ADX(dataframe)

        bollinger = qtpylib.bollinger_bands(dataframe['close'], window=20, stds=2)
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_middleband'] = bollinger['mid']
        dataframe['bb_upperband'] = bollinger['upper']

        return dataframe

    def buy_trend(self, dataframe: DataFrame) -> bool:
        dataframe.loc[
            (
                    (dataframe['adx'] > 30) &
                    (dataframe['tema'] < dataframe['bb_middleband']) &
                    (dataframe['tema'] > dataframe['tema'].shift(1)) &
                    (dataframe['sma_200'] > dataframe['close'])

            ),
            'buy'] = 1
        buy = dataframe.iloc[-1, dataframe.columns.get_loc("buy")]
        return True if buy else False

    def sell_trend(self, dataframe: DataFrame) -> bool:
        dataframe.loc[
            (
                    (dataframe['adx'] > 70) &
                    (dataframe['tema'] > dataframe['bb_middleband']) &
                    (dataframe['tema'] < dataframe['tema'].shift(1))
            ),
            'sell'] = 1
        sell = dataframe.iloc[-1, dataframe.columns.get_loc("sell")]
        return True if sell else False

    def action(self):
        self.update_indicators(self.data)
        if self.buy_trend(self.data) and not self.is_trading:
            return 'BUY'
        if self.sell_trend(self.data) and self.is_trading:
            return 'SELL'
        return None
