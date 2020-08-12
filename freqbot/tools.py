from threading import Thread
import numpy as np
import time


class OrderMetadata:
    def __init__(self):
        self.quantity = None
        self.start_price = None
        self.end_price = None
        self.pair = None
        self.start_time = None
        self.end_time = None
        self.roi = dict()
        self.stoploss = None
        self.sell_cause = None
        # self.is_trading = False  # may be is unnecessarily

    def set_roi_stoploss(self, roi: dict, stoploss):
        self.roi = {float(key): value for key, value in roi.items()}
        self.stoploss = stoploss

    def set_time(self, action):
        if action == 'BUY':
            self.start_time = time.perf_counter()
        else:
            self.end_time = time.perf_counter()

    def roi_stoploss_check(self, price):
        diff = time.perf_counter() - self.start_time
        keys = np.array(list(self.roi.keys()))
        key = np.min(keys[keys - diff > 0])
        profit_price = self.start_price * (1 + self.roi[key])
        if price > profit_price:
            self.set_time('SELL')
            self.sell_cause = 'ROI'
            return True
        if price < self.start_price * (1 + self.stoploss):
            self.set_time('SELL')
            self.sell_cause = 'STOPLOSS'
            return True
        return False

    @staticmethod
    def get_price(order):
        cost = 0
        quantity = 0
        for fill in order['fills']:
            cost += float(fill['price']) * float(fill['qty'])
            quantity += float(fill['qty'])
        return cost / quantity

    def handle_order(self, order):
        self.pair = order['symbol']
        if order['type'] == 'MARKET':
            if order['side'] == 'BUY':
                self.start_price = self.get_price(order)
            else:
                self.end_price = self.get_price(order)
        if order['side'] == 'SELL':
            self.sell_cause = 'SELL SIGNAL'

    def flush(self):
        self.quantity = None
        self.start_price = None
        self.end_price = None
        self.pair = None
        self.start_time = None
        self.end_time = None
        self.sell_cause = None


class LogHandler:
    def __init__(self):
        self.order_list = list()

    def add(self, order):
        self.order_list.append(order)
        order.flush()
