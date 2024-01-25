import logging
import os
import sys


def get_logger() -> logging.Logger:
    logger = logging.getLogger('offsets-db-api')
    worker_id = os.environ.get('APP_WORKER_ID', '')

    if not logger.handlers:
        handler = logging.StreamHandler(stream=sys.stdout)
        if worker_id != '':
            handler.setFormatter(
                logging.Formatter(f'[%(name)s] [worker {worker_id}] [%(levelname)s] %(message)s')
            )
        else:
            handler.setFormatter(logging.Formatter('[%(name)s] [%(levelname)s] %(message)s'))
        logger.addHandler(handler)

    logger.setLevel(logging.DEBUG)
    return logger
