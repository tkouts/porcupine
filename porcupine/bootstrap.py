import os
import sys
import signal
import time
import argparse
import logging

from sanic import Sanic

from . import __version__
from .config import settings
from .log import setup_daemon_logging
from porcupine.apps.main import Porcupine

PID_FILE = '.pid'
sanic = Sanic()


def set_pid():
    with open(PID_FILE, 'w') as pid_file:
        pid_file.write(str(os.getpgid(os.getpid())))


def get_pid():
    with open(PID_FILE, 'r') as pid_file:
        return int(pid_file.read())


def fork():
    setup_daemon_logging()
    out = open('/dev/null', 'w')
    sys.stdout = out
    sys.stderr = out
    pid = os.fork()
    if pid:
        set_pid()
    return pid


def stop():
    try:
        pid = get_pid()
    except IOError:
        return
    try:
        os.killpg(pid, signal.SIGINT)
    except OSError:
        # porcupine is not running
        pass
    else:
        # wait for process to be killed
        while True:
            try:
                os.killpg(pid, 0)
                time.sleep(0.1)
            except OSError:
                break


def start(args):
    if args.daemon:
        pid = fork()
        if pid:
            sys.exit()
    elif args.stop:
        stop()
        sys.exit()
    elif args.graceful:
        stop()
        pid = fork()
        if pid:
            sys.exit()

    logging.info('Starting Porcupine %s', __version__)
    # register main app
    Porcupine.install()
    sanic.run(host=settings['host'],
              port=settings['port'],
              workers=settings['workers'])


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host',
                        help='host name for incoming connections')
    parser.add_argument('--port',
                        help='port listening for incoming connections',
                        type=int)
    parser.add_argument('--workers',
                        help='number of worker processes',
                        type=int)
    parser.add_argument('--daemon',
                        help='run porcupine as a background service',
                        action='store_true')
    parser.add_argument('--stop',
                        help='stop porcupine',
                        action='store_true')
    parser.add_argument('--graceful',
                        help='restart porcupine',
                        action='store_true')
    args = parser.parse_args()

    # override settings values
    for arg in ('host', 'port', 'workers'):
        override = hasattr(args, arg) and getattr(args, arg)
        if override:
            settings[arg] = override

    start(args)
