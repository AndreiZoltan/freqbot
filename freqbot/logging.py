import logging
import os
from time import strftime


def get_logger(mode: str):
    assert mode in ['trade', 'backtest', 'dry_run']

    # create logger with 'spam_application'
    logger = logging.getLogger(mode)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    path = os.path.abspath(__file__)
    path = "/".join(path.split('/')[:-2]) + '/logging/' + mode + '/' + strftime("%Y_%m_%d-%H_%M_%S") + '.log'
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
