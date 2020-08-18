import numpy as np
import pandas as pd
import os
import freqml as fm
import concurrent.futures
from typing import Union, List, Dict, NoReturn
from freqml import *

from freqbot import TradingBot
from freqbot.tradingbot import OrderMetadata
from freqbot.database import DataHandler
from freqbot.tools import timedelta2seconds
from freqbot.algos import BasicAlgorithm
from freqbot.logging import get_logger


class BacktestingBot(TradingBot):
    def __init__(self, key, secret):
        super().__init__(key, secret)
        self.pairs: List[str] = list()
        self.algos: List[str] = list()
        self.tick2algo: Dict[str, List[BasicAlgorithm]] = dict()
        self.tick_pair_frames: Dict[str, List[pd.Dataframes]] = dict()

    def trade(self, pair, days, algorithm, override: bool = True, stake_amount: int = 10) -> NoReturn:
        raise AttributeError

    def algo_name(self, algorithm: BasicAlgorithm) -> str:
        return type(algorithm).__name__

    def set_metadata(self, pairs: List[str], stake_amount: int, algorithms: List[BasicAlgorithm]):

        def get_tick_type(algorithm: BasicAlgorithm) -> str:
            return algorithm.tick_type + '_' + str(algorithm.tick_size)

        self.pairs = pairs
        self.stake_amount = stake_amount
        for algo in algorithms:
            algo_name = self.algo_name(algo)
            self.algos.append(algo_name)
            algo.roi = {float(key): value for key, value in algo.roi.items()}
            algo.roi[np.inf] = 0
            tick_type = get_tick_type(algo)
            self.tick_pair_frames[tick_type] = list()
            if tick_type in self.tick2algo:
                self.tick2algo[tick_type].append(algo)
            else:
                self.tick2algo[tick_type] = [algo]

    def update_tick_pair_frames(self, new_data: pd.DataFrame, pair: str):
        for tick_type in self.tick_pair_frames:
            t_type, t_size = tick_type.split('_')
            states = getattr(new_data.bars, t_type)(int(t_size))
            states.name = pair
            self.tick_pair_frames[tick_type].append(states)

    def get_historical_data(self, pairs: List[str], days: int, override: bool) -> NoReturn:
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-2]) + '/historical_data/'
        for pair in pairs:
            new_data = fm.load_dataset(client=self.client,
                                       pair=pair,
                                       days=days,
                                       path=path,
                                       override=override)
            self.logger.info(pair + ' DATA FOR ' + str(days) + ' DAY(S) WAS DOWNLOADED')
            self.update_tick_pair_frames(new_data, pair)   # multiprocessing

    def df_gen(self, df: pd.DataFrame) -> pd.DataFrame:
        n = df.shape[0]
        for i in range(n):
            yield df.iloc[i: i + 1]

    def roi_stoploss_backtest_check(self, meta: OrderMetadata, state: pd.DataFrame,
                                    roi: dict, stoploss: int) -> Union[float, None]:

        if not meta.start_price:
            return False

        price = state['close'][0]

        diff = state.index[0] - meta.start_time
        diff = timedelta2seconds(diff)
        keys = np.array(list(roi.keys()))
        key = np.min(keys[keys * 60 - diff > 0])
        profit_price = meta.start_price * (1 + roi[key])
        if price > profit_price:
            meta.set_sell_reason('ROI')
            return profit_price
        loss_price = meta.start_price * (1 + stoploss)
        if price < loss_price:
            meta.set_sell_reason('STOPLOSS')
            return loss_price
        return None

    def buy_handling(self, meta: OrderMetadata, state: pd.DataFrame, stake_amount: int) -> OrderMetadata:
        commission = 0.00075
        start_price = float(state['close'])
        meta.set_start_price(start_price)
        meta.set_quantity(stake_amount / start_price)
        meta.fee = stake_amount * commission
        meta.start_time = state.index[0]
        meta.ctime = state.index[0].ctime()
        return meta

    def sell_handling(self, meta: OrderMetadata, state: pd.DataFrame, end_price: float = None) -> OrderMetadata:
        commission = 0.00075
        if not end_price:
            end_price = float(state['close'])
        meta.set_end_price(end_price)
        meta.fee += meta.get_quantity() * end_price * commission
        meta.end_time = timedelta2seconds(state.index[0] - meta.start_time)
        meta.start_time = 0
        return meta

    def backtest_algo_pair(self, algo: BasicAlgorithm, pair: pd.DataFrame, stake_amount: int, name: str) -> NoReturn:
        is_trading = False
        data_handler = DataHandler('backtest')
        meta = OrderMetadata()
        meta.order_type = 'MARKET'
        meta.set_algorithm_name(algo)
        meta.pair = name
        for state in self.df_gen(pair):
            assert state.shape[0] == 1

            if is_trading:
                end_price = self.roi_stoploss_backtest_check(meta, state, algo.roi, algo.stoploss)
                if end_price:
                    meta = self.sell_handling(meta, state, end_price)
                    data_handler.update(vars(meta))
                    meta.flush()
                    meta.pair = name
                    meta.order_type = 'MARKET'
                    is_trading = False

            algo.set_state(state)
            action = algo.action(is_trading)

            if action:
                assert action in ['BUY', 'SELL']
                if action == 'BUY':
                    meta = self.buy_handling(meta, state, stake_amount)
                    is_trading = True
                else:
                    meta.set_sell_reason('SELL SIGNAL')
                    meta = self.sell_handling(meta, state)
                    data_handler.update(vars(meta))
                    meta.flush()
                    meta.pair = name
                    meta.order_type = 'MARKET'
                    is_trading = False

        self.logger.info(self.algo_name(algo) + ' WAS TESTED ON ' + name)

    def backtest(self, pairs: List[str], algorithms: List[BasicAlgorithm], days: int = 3,
                 override: bool = True, stake_amount: int = 10):
        self.logger = get_logger('backtest')
        self.set_metadata(pairs, stake_amount, algorithms)
        self.data_handler = DataHandler('backtest')
        self.data_handler.drop_all_tables()
        self.get_historical_data(pairs, days, override)
        self.logger.info('ALL DATA WAS DOWNLOADED AND PROCESSED TO NEEDED FORMAT')

        # sqlite3.Connection is not pickable
        del self.data_handler

        with concurrent.futures.ProcessPoolExecutor() as executor:
            for tick_type in self.tick_pair_frames.keys():
                for algo in self.tick2algo[tick_type]:
                    for pair in self.tick_pair_frames[tick_type]:
                        executor.submit(self.backtest_algo_pair, algo, pair, stake_amount, pair.name)
