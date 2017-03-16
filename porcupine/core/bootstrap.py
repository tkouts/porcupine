import argparse
import logging
import signal
import sys
import time

import os

from porcupine import __version__
from porcupine.apps.main import main
from porcupine.config import settings
from .log import setup_daemon_logging
from .server import server

PID_FILE = '.pid'


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
    if args.debug:
        settings['log']['level'] = logging.DEBUG
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
    # register apps
    apps = [main]
    for app in apps:
        server.blueprint(app, url_prefix=app.name)
    server.run(host=settings['host'],
               port=settings['port'],
               workers=settings['workers'],
               debug=args.debug)


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
    parser.add_argument('--debug',
                        help='enable debug mode',
                        action='store_true')
    args = parser.parse_args()

    # override settings values
    for arg in ('host', 'port', 'workers'):
        override = getattr(args, arg, None)
        if override:
            settings[arg] = override

    start(args)
