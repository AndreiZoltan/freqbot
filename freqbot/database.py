import sqlite3 as sl
import freqbot as fb
import pandas as pd
import os


class DataHandler:
    def __init__(self, filename):
        self.db = None
        self.connect(filename)
        self.create_tables()

    def connect(self, filename: str):
        path = os.path.abspath(__file__)
        path = "/".join(path.split('/')[:-2]) + '/databases/'
        self.path = path
        self.db = sl.connect(self.path + filename + '.db')

    def create_tables(self):
        with self.db:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS MAIN (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME NOT NULL,
                    duration INTEGER NOT NULL,
                    start_price FLOAT NOT NULL,
                    end_price FLOAT NOT NULL,
                    ratio FLOAT NOT NULL,
                    sell_reason VARCHAR NOT NULL,
                    income FLOAT NOT NULL,
                    fee FLOAT NOT NULL,
                    pair VARCHAR NOT NULL,
                    algorithm VARCHAR NOT NULL,
                    stake_amount FLOAT NOT NULL,
                    order_type VARCHAR NOT NULL,
                    limit_type VARCHAR
                );
            """)
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS PAIR (
                    pair VARCHAR NOT NULL PRIMARY KEY,
                    num_loss INTEGER NOT NULL,
                    num_profit INTEGER NOT NULL,
                    num_total INTEGER NOT NULL,
                    ratio_num FLOAT,
                    loss FLOAT NOT NULL,
                    profit FLOAT NOT NULL,
                    total FLOAT NOT NULL,
                    average_duration FLOAT
                );
            """)
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS ALGO (
                    algo VARCHAR NOT NULL PRIMARY KEY,
                    num_loss INTEGER NOT NULL,
                    num_profit INTEGER NOT NULL,
                    num_total INTEGER NOT NULL,
                    ratio_num FLOAT,
                    loss FLOAT NOT NULL,
                    profit FLOAT NOT NULL,
                    total FLOAT NOT NULL,
                    average_duration FLOAT
                );
            """)


    def get_main_data(self, metadata: dict):
        main_data = list()
        main_data.append(metadata['start_time'])                                         # start_time
        main_data.append((metadata['end_time'] - metadata['start_time']) / 60)           # duration
        main_data.append(metadata['start_price'])                                        # start_price
        main_data.append(metadata['end_price'])                                          # end_price
        main_data.append(metadata['end_price'] / metadata['start_price'])                # ratio
        main_data.append(metadata['sell_reason'])                                        # sell_reason
        main_data.append()
        main_data.append(metadata['fee'])                                                # fee


    def update_main(self, metadata):
        sql = 'INSERT INTO MAIN \
        (start_time, duration, start_price, end_price, ratio, sell_reason, income, fee, pair,\
         algorithm, stake_amount, order_type, limit_type) \
         values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        data = [
            (metadata['start_time'], self.get_duration(metadata)), metadata['start_price'], metadata['end_price'],
            metadata
        ]

    def update_pair(self):
        pass

    def update_algo(self):
        pass

    def update(self, metadata: dict):
        """
        update is used to updated 3 tables (MAIN, PAIR, ALGO)
        :param metadata: is dict that consist of following keys: quantity, start_price, end_price, pair, start_time,
        end_time, sell_cause
        :return: trade function has no return but it updates tables
        """
        self.update_main(metadata)
        self.update_pair(metadata)
        self.update_algo(metadata)

    def drop_table(self):
        with self.db:
            self.db.execute("""
            DROP TABLE IF EXISTS SKILL""")
#
#
# class DataBase:
#     def __init__(self):
#         self.backtest = DataHandler("backtest")
#         self.dry_trade = DataHandler("dry_trade")
#         self.trade = DataHandler("trade")
#
#     def drop_table(self):
#         with self.trade:
#             self.trade.execute("""
#             DROP TABLE IF EXISTS USER""")
#
    def insert(self):
        sql = 'INSERT INTO USER (name, age) values(?, ?)'
        data = [
            ('Alice', 21),
            ('Bob', 22),
            ('Chris', 23)
        ]
        with self.trade:
            self.trade.executemany(sql, data)
#
#     def query(self):
#         with self.trade:
#             data = self.trade.execute("SELECT * FROM USER WHERE age <= 22")
#             for row in data:
#                 print(row)
#
#     def insert_pandas(self):
#         df_skill = pd.DataFrame({
#             'user_id': [1, 1, 2, 2, 3, 3, 3],
#             'skill': ['Network Security', 'Algorithm Development', 'Network Security', 'Java', 'Python', 'Data Science',
#                       'Machine Learning']
#         })
#         df_skill.to_sql('SKILL', self.trade)
#
#     def add(self, order):
#         pass
#         # self.order_list.append(order)


if __name__ == '__main__':
    #dh = DataHandler('trade')
    #dh.drop_table()

    order = fb.OrderMetadata()
    print(dir(order))