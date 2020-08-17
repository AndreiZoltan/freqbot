def r(value: float, precision: int = 4) -> float:
    return float("{:0.0{}f}".format(value, precision))


def time2stamp(datetime) -> int:
    return int(datetime.value / 1000000)
