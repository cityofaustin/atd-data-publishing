import logging
from logging.handlers import TimedRotatingFileHandler

def timed_rotating_log(path, when='D', interval=1, backupCount=5):
    logger = logging.getLogger('Rotating Log')
    
    logger.setLevel(logging.INFO)
 
    handler = TimedRotatingFileHandler(
        path,
        when=when,
        interval=interval,
        backupCount=backupCount
    )

    logger.addHandler(handler)

    return logger