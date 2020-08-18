import numpy as np


def r(value: float, precision: int = 4) -> float:
    return float("{:0.0{}f}".format(value, precision))


def time2stamp(datetime) -> int:
    return int(datetime.value / 1000000)


def timedelta2seconds(delta: np.timedelta64) -> int:
    return np.array([delta], dtype="timedelta64[s]")[0].item().total_seconds()
