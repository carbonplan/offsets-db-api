import logging
import sys


def get_logger() -> logging.Logger:
    logger = logging.getLogger('offsets-db')

    if not logger.handlers:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(name)s - %(message)s'))
        logger.addHandler(handler)

    logger.setLevel(logging.DEBUG)
    return logger
