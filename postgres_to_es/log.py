import logging

from settings import Config

config = Config.parse_file('config.json')

def get_logger() -> logging.Logger:
    """Get and set logging for debug and measure performance."""
    logger = logging.getLogger(__name__)
    logger.setLevel(config.log.logger_level)
    handler = logging.StreamHandler()
    log_format = '%(asctime)s %(levelname)s -- %(message)s'
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

