import logging
import logging.handlers
from .config import settings

LOG_FILE = 'porcupine.log'

log = logging.root
log_settings = settings['log']
formatter = logging.Formatter(log_settings['format'])
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(log_settings['level'])


def setup_daemon_logging():
    # remove stream handler
    log.removeHandler(log.handlers[0])
    # add file handler
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        'a',
        log_settings['maxbytes'],
        log_settings['backups'])
    handler.setFormatter(formatter)
    log.addHandler(handler)
