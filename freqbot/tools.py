def r(value: float) -> float:
    return float("{:0.0{}f}".format(value, 2))


def time2stamp(datetime) -> int:
    return int(datetime.value / 1000000)
