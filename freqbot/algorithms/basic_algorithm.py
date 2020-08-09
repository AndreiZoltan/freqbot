import pandas as pd


class BasicAlgorithm:
    def __init__(self, tick_type: str, size):
        self.tick_type = tick_type
        self.size = size
        self.state = pd.DataFrame()

    def get_state(self, state):
        print(state)
        self.state = state

    def action(self):
        pass
