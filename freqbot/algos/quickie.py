from freqbot.algos import BasicAlgorithm
from pandas import DataFrame
import pandas as pd
import talib as ta


class Quickie(BasicAlgorithm):
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        super(Quickie, self).__init__(tick_type, tick_size, order_type)

    def update_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['macdhist'] = macd['macdhist']

        dataframe['tema'] = ta.TEMA(dataframe, timeperiod=9)
        dataframe['sma_200'] = ta.SMA(dataframe, timeperiod=200)
        dataframe['sma_50'] = ta.SMA(dataframe, timeperiod=200)

        dataframe['adx'] = ta.ADX(dataframe)

        return dataframe

    def buy_trend(self, dataframe: DataFrame, metadata: dict) -> bool:
        buy = dataframe.loc[
            (
                    (dataframe['adx'] > 30) &
                    (dataframe['tema'] < dataframe['bb_middleband']) &
                    (dataframe['tema'] > dataframe['tema'].shift(1)) &
                    (dataframe['sma_200'] > dataframe['close'])

            )].shape[0]
        return True if buy else False

    def sell_trend(self, dataframe: DataFrame, metadata: dict) -> bool:
        sell = dataframe.loc[
            (
                    (dataframe['adx'] > 70) &
                    (dataframe['tema'] > dataframe['bb_middleband']) &
                    (dataframe['tema'] < dataframe['tema'].shift(1))
            )].shape[0]
        return True if sell else False
