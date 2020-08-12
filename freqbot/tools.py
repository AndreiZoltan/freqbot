from threading import Thread
import time


class LogHandler:
    def __init__(self):
        pass

    def get_price(self, order):
        cost = 0
        quantity = 0
        for fill in order['fills']:
            cost += float(fill['price']) * float(fill['qty'])
            quantity += float(fill['qty'])
        return cost / quantity