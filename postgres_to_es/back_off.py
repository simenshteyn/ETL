import logging
import time
from functools import wraps


def backoff(ExceptionToCheck, logger: logging.Logger,
            tries=16, delay=0.1, backoff=2):
    """Retry calling the decorated function using an exponential backoff."""

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    logger.warning("%s, Retrying in %d seconds...", e, mdelay)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry
