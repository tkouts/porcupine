import logging
import logging.handlers
from .config import settings

LOG_FILE = 'porcupine.log'

log = logging.root
formatter = logging.Formatter(settings['log']['format'])
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(settings['log']['level'])


def setup_daemon_logging():
    # remove stream handler
    log.removeHandler(log.handlers[0])
    # add file handler
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        'a',
        int(settings['log']['maxbytes']),
        int(settings['log']['backups']))
    handler.setFormatter(formatter)
    log.addHandler(handler)
