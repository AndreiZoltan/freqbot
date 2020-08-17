import sqlite3
import os
from numpy import heaviside, maximum
from typing import Union, NoReturn
import time

from freqbot.tools import r


class DataHandler:
    def __init__(self, filename):
        self.connection = self.connect(filename)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.create_tables()

    @staticmethod
    def connect(filename: str) -> sqlite3.Connection:
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-2]) + '/databases/'
        return sqlite3.connect(path + filename + '.db', check_same_thread=False)

    def create_tables(self) -> NoReturn:
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS MAIN (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME NOT NULL,
                    duration INTEGER NOT NULL,
                    start_price REAL NOT NULL,
                    end_price REAL NOT NULL,
                    ratio REAL NOT NULL,
                    sell_reason VARCHAR NOT NULL,
                    income REAL NOT NULL,
                    fee REAL NOT NULL,
                    pair VARCHAR NOT NULL,
                    algorithm VARCHAR NOT NULL,
                    stake_amount REAL NOT NULL,
                    order_type VARCHAR NOT NULL,
                    limit_type VARCHAR
                );
            """)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS PAIR (
                    pair VARCHAR NOT NULL PRIMARY KEY,
                    num_loss INTEGER NOT NULL,
                    num_profit INTEGER NOT NULL,
                    num_total INTEGER NOT NULL,
                    ratio_num REAL,
                    loss REAL NOT NULL,
                    profit REAL NOT NULL,
                    ratio REAL NOT NULL,
                    total REAL NOT NULL,
                    av_duration REAL
                );
            """)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS ALGO (
                    algo VARCHAR NOT NULL PRIMARY KEY,
                    num_loss INTEGER NOT NULL,
                    num_profit INTEGER NOT NULL,
                    num_total INTEGER NOT NULL,
                    ratio_num REAL,
                    loss REAL NOT NULL,
                    profit REAL NOT NULL,
                    ratio REAL NOT NULL,
                    total REAL NOT NULL,
                    av_duration REAL
                );
            """)

    def get_income(self, metadata: dict) -> float:
        income = (metadata['end_price'] - metadata['start_price']) * metadata['quantity']
        income -= metadata['fee']
        return income

    def get_duration(self,  metadata) -> float:
        return (metadata['end_time'] - metadata['start_time']) / 60

    def get_av_duration(self, old_data, new_data, num_total) -> float:
        return ((num_total - 1) * old_data['av_duration'] + self.get_duration(new_data)) / num_total

    def get_pair_algo_line(self, key: str, table) -> Union[sqlite3.Row, bool]:
        assert table in ['PAIR', 'ALGO']
        if table == 'PAIR':
            self.cursor.execute('SELECT * FROM PAIR WHERE pair=?', (key, ))
        else:
            self.cursor.execute('SELECT * FROM ALGO WHERE algo=?', (key, ))
        rows = self.cursor.fetchall()
        assert len(rows) == 1 or len(rows) == 0
        if len(rows) == 1:
            return rows[0]
        else:
            return False

    def get_main_data(self, metadata: dict) -> tuple:
        main_data = list()
        main_data.append(metadata['ctime'])                                              # start_time
        main_data.append(r(self.get_duration(metadata)))                                 # duration
        main_data.append(r(metadata['start_price']))                                     # start_price
        main_data.append(r(metadata['end_price']))                                       # end_price
        main_data.append(r(metadata['end_price'] / metadata['start_price']))             # ratio
        main_data.append(metadata['sell_reason'])                                        # sell_reason
        main_data.append(r(self.get_income(metadata), 6))                                # income
        main_data.append(r(metadata['fee'], 6))                                          # fee
        main_data.append(metadata['pair'])                                               # pair
        main_data.append(metadata['algorithm_name'])                                     # algorithm
        main_data.append(r(metadata['start_price'] * metadata['quantity']))              # stake_amount
        main_data.append(metadata['order_type'])                                         # order type
        main_data.append(metadata['limit_type'])                                         # limit_type
        return tuple(main_data)

    def get_pair_algo_data(self, metadata: dict, table: str) -> tuple:
        assert table in ['ALGO', 'PAIR']
        new_data = list()
        if table == 'ALGO':
            old_data = self.get_pair_algo_line(metadata['algorithm_name'], table)           # index
            new_data.append(metadata['algorithm_name'])
        else:
            old_data = self.get_pair_algo_line(metadata['pair'], table)                     # index
            new_data.append(metadata['pair'])
        income = self.get_income(metadata)
        if not old_data:
            num_loss = heaviside(-income, 0)
            new_data.append(num_loss)                                                       # num_loss
            num_profit = heaviside(income, 1)
            new_data.append(num_profit)                                                     # num_profit
            new_data.append(num_profit + num_loss)                                          # num_total
            ratio_n = r(num_profit / (num_loss + num_profit))
            new_data.append(ratio_n)                                                        # ratio_n
            loss = maximum(-income, 0)
            new_data.append(r(loss, 6))                                                     # loss
            profit = maximum(income, 0)
            new_data.append(r(profit, 6))                                                   # profit
            ratio = profit / (loss + profit)
            new_data.append(r(ratio))                                                       # ratio
            new_data.append(r(profit - loss))                                               # total
            new_data.append(r(self.get_duration(metadata)))                                 # av_duration
        else:
            num_loss = old_data['num_loss'] + heaviside(-income, 0)
            new_data.append(num_loss)                                                       # num_loss
            num_profit = old_data['num_profit'] + heaviside(income, 1)
            new_data.append(num_profit)                                                     # num_profit
            num_total = num_loss + num_profit
            new_data.append(num_total)                                                      # num_total
            ratio_n = num_profit / (num_loss + num_profit)
            new_data.append(r(ratio_n))                                                     # ratio_n
            loss = old_data['loss'] + maximum(-income, 0)
            new_data.append(r(loss))                                                        # loss
            profit = old_data['profit'] + maximum(income, 0)
            new_data.append(r(profit))                                                      # profit
            ratio = profit / (loss + profit)
            new_data.append(r(ratio))                                                       # ratio
            new_data.append(r(profit - loss))                                               # total
            av_duration = self.get_av_duration(old_data, metadata, num_total)
            new_data.append(r(av_duration))                                                 # av_duration
        return tuple(new_data)

    def update_main(self, metadata: dict) -> NoReturn:
        sql = 'INSERT INTO MAIN \
        (start_time, duration, start_price, end_price, ratio, sell_reason, income, fee, pair,\
         algorithm, stake_amount, order_type, limit_type) \
         values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        data = [self.get_main_data(metadata)]
        with self.connection:
            self.connection.executemany(sql, data)

    def update_pair(self, metadata: dict) -> NoReturn:
        sql = 'REPLACE INTO PAIR \
        (pair, num_loss, num_profit, num_total, ratio_num, loss, profit, ratio, total, av_duration) ' \
        'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
        data = [self.get_pair_algo_data(metadata, 'PAIR')]
        with self.connection:
            self.connection.executemany(sql, data)

    def update_algo(self, metadata: dict) -> NoReturn:
        sql = 'REPLACE INTO ALGO \
        (algo, num_loss, num_profit, num_total, ratio_num, loss, profit, ratio, total, av_duration) ' \
        'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
        data = [self.get_pair_algo_data(metadata, 'ALGO')]
        with self.connection:
            self.connection.executemany(sql, data)

    def update(self, metadata: dict) -> NoReturn:
        """
        update is used to updated 3 tables (MAIN, PAIR, ALGO)
        :param metadata: is dict that consist of following keys: quantity, start_price, end_price, pair, start_time,
        end_time, sell_cause
        :return: trade function has no return but it updates tables
        """
        self.update_main(metadata)
        self.update_pair(metadata)
        self.update_algo(metadata)

    def drop_all_tables(self) -> NoReturn:
        with self.connection:
            self.connection.execute("""
            DROP TABLE IF EXISTS MAIN""")
            self.connection.execute("""
            DROP TABLE IF EXISTS PAIR""")
            self.connection.execute("""
            DROP TABLE IF EXISTS ALGO""")


if __name__ == '__main__':
    dh = DataHandler('trade')
    # dh.drop_table()

    # order = fb.OrderMetadata()
    order = {'quantity': 300, 'start_price': 8.5, 'end_price': 9,
             'pair': "LOLKUK", 'start_time': 1234, 'end_time': 2100,
             'sell_reason': 'SELL SIGNAL', 'fee': 0.1, 'order_type': "MARKET",
             'limit_type': None, 'algorithm_name': "MUMBA_v4"}

    print('START')
    print(time.ctime())
    # dh.drop_table()
    dh.update(order)
    print(time.ctime())
