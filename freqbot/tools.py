from threading import Thread
import time


class RoiTimer(Thread):
    def __init__(self, roi_dict: dict, start_price):
        Thread.__init__(self)
        self.roi_dict = roi_dict
        self.start_price = start_price
        self.profit_price = 2 * start_price

    def run(self):
        old_sleep_time = 0
        for _ in range(len(self.roi_dict)):
            sleep_time = min(self.roi_dict.keys(), key=float)
            percent = self.roi_dict[sleep_time]
            profit_price = self.start_price * (1 + percent)
            print(profit_price, "  ", time.strftime("%H:%M:%S", time.localtime()))
            time.sleep(int(sleep_time) - int(old_sleep_time))
            old_sleep_time = sleep_time
            del self.roi_dict[sleep_time]