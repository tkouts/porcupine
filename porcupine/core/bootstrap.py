import argparse
import sys
import os
import logging
import logging.config

from porcupine import __version__
from porcupine.core.server import server
from porcupine import apps
from .log import porcupine_log
from .loader import install_apps
from .services.blueprint import services_blueprint, services
from .daemon import Daemon


def run_server(log_config, debug=False):
    # register services blueprint
    server.blueprint(services_blueprint)
    porcupine_log.info('Starting Porcupine {0}'.format(__version__))
    # prepare services
    porcupine_log.info('Preparing services')
    for service in services:
        service.prepare(server)
    # install native apps
    install_apps(apps.__path__, prefix='porcupine.apps.')
    current_dir = os.getcwd()
    porcupine_path = os.path.dirname(
        os.path.dirname(sys.modules['porcupine'].__file__))
    if not current_dir.startswith(porcupine_path):
        # install user apps
        install_apps([current_dir])
        # check if there is a static directory
        static_dir_path = os.path.join(current_dir, 'static')
        if os.path.isdir(static_dir_path):
            server.static('/', static_dir_path)

    server.run(host=server.config.HOST,
               port=int(server.config.PORT),
               workers=int(server.config.WORKERS),
               debug=debug,
               backlog=150,
               log_config=log_config)


class PorcupineDaemon(Daemon):
    def __init__(self, log_config, debug=False):
        super().__init__(server.config.PID_FILE)
        self.debug = debug
        self.log_config = log_config

    def run(self):
        run_server(self.log_config, debug=self.debug)


def start(args):
    if args.debug:
        server.config.LOG_LEVEL = logging.DEBUG

    log_config = server.get_log_config(
        log_to_files=args.daemon or args.graceful)
    # load config
    logging.config.dictConfig(log_config)

    if args.daemon or args.stop or args.graceful:
        # daemon commands
        daemon = PorcupineDaemon(log_config, debug=args.debug)
        if args.daemon:
            daemon.start()
        elif args.stop:
            daemon.stop()
        elif args.graceful:
            daemon.restart()
        sys.exit()
    else:
        run_server(log_config, debug=args.debug)


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
