import argparse
import logging
import sys
import asyncio

from porcupine import __version__
from porcupine.apps.resources import resources
from porcupine.config import settings
from . import log
from .server import server
from .services import services
from .daemon import Daemon

PID_FILE = '/tmp/porcupine.pid'


def run_server(debug=False, loop=None):
    log.porcupine_log.info('Starting Porcupine %s', __version__)
    server.run(host=settings['host'],
               port=settings['port'],
               workers=settings['workers'],
               loop=loop,
               debug=debug)


class PorcupineDaemon(Daemon):
    def __init__(self, debug=False):
        super().__init__(PID_FILE)
        self.debug = debug

    def run(self):
        loop = asyncio.get_event_loop()
        run_server(self.debug, loop=loop)


def start(args):
    if args.debug:
        settings['log']['level'] = logging.DEBUG

    is_multi_process = settings['workers'] > 1
    log.setup(args.daemon, is_multi_process)

    before_start = server.listeners['before_server_start']
    if is_multi_process:
        before_start.append(log.setup_mp)

    # register services blueprint
    server.blueprint(services)

    # locate apps
    apps = [resources]
    # register apps
    for app in apps:
        server.blueprint(app, url_prefix=app.name)

    try:
        if args.daemon or args.stop or args.graceful:
            # daemon commands
            daemon = PorcupineDaemon(debug=args.debug)
            if args.daemon:
                daemon.start()
            elif args.stop:
                daemon.stop()
            elif args.graceful:
                daemon.restart()
            sys.exit()
        else:
            run_server(debug=args.debug)
    finally:
        log.shutdown()


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
    for arg in ('host', 'port', 'workers', 'daemon'):
        override = getattr(args, arg, None)
        if override:
            settings[arg] = override
    # start the server
    start(args)
