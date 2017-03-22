import logging
import threading
from multiprocessing import Queue
import logging.handlers
from sanic.log import log as sanic_log
from porcupine.config import settings

LOG_FILE = 'porcupine.log'
log_settings = settings['log']
sanic_log.level = log_settings['level']
porcupine_log = logging.getLogger('porcupine')

mp_log_queue = None
mp_log_thread = None


def logger_thread():
    while True:
        record = mp_log_queue.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)


def setup(daemon, multiprocess):
    root = logging.getLogger()
    formatter = logging.Formatter(log_settings['format'])
    if daemon:
        log_handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            'a',
            log_settings['max_bytes'],
            log_settings['backups'])
    else:
        log_handler = logging.StreamHandler()
    log_handler.setFormatter(formatter)
    root.addHandler(log_handler)
    root.setLevel(log_settings['level'])
    if multiprocess:
        global mp_log_queue, mp_log_thread
        mp_log_queue = Queue()
        mp_log_thread = threading.Thread(target=logger_thread)
        mp_log_thread.start()


def setup_mp(*args):
    qh = logging.handlers.QueueHandler(mp_log_queue)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.removeHandler(root.handlers[0])
    root.addHandler(qh)


def shutdown():
    if mp_log_queue is not None:
        mp_log_queue.put(None)
        mp_log_thread.join()
