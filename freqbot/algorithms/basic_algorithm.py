import pandas as pd


class BasicAlgorithm:
    def __init__(self, tick_type: str, tick_size, order_type: str = 'MARKET'):
        self.tick_type = tick_type
        self.tick_size = tick_size
        self.order_type = order_type
        self.price = 0
        self.state = pd.DataFrame()

    def get_state(self, state):
        print(state)
        self.state = state

    def action(self):
        pass
