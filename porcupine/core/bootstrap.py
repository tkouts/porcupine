import argparse
import sys
import os
import logging
import logging.config

from porcupine import __version__
from porcupine.core.server import server
from .log import porcupine_log
from .services import prepare_services
from .daemon import Daemon


def run_server(debug=False):
    porcupine_log.info('Starting Porcupine {0}'.format(__version__))

    # prepare services
    porcupine_log.info('Preparing services')
    prepare_services(server)

    server.run(host=server.config.HOST,
               port=int(server.config.PORT),
               workers=int(server.config.WORKERS),
               debug=debug,
               backlog=150,
               access_log=server.config.LOG_ACCESS)


class PorcupineDaemon(Daemon):
    def __init__(self, scan_dir, debug=False):
        super().__init__(server.config.PID_FILE)
        self.debug = debug
        self.scan_dir = scan_dir

    def run(self):
        os.chdir(self.scan_dir)
        run_server(debug=self.debug)


def start(args):
    if args.debug:
        server.config.LOG_LEVEL = logging.DEBUG

    log_config = server.get_log_config(
        log_to_files=args.daemon or args.graceful)
    # load config
    logging.config.dictConfig(log_config)

    if args.daemon or args.stop or args.graceful:
        # daemon commands
        current_dir = os.getcwd()
        daemon = PorcupineDaemon(current_dir, debug=args.debug)
        if args.daemon:
            daemon.start()
        elif args.stop:
            daemon.stop()
        elif args.graceful:
            daemon.restart()
        sys.exit()
    else:
        run_server(debug=args.debug)


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
            server.config[arg.upper()] = override
    # start the server
    start(args)
