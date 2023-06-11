import os
import sys

from sanic import Sanic
from sanic.request import Request
from orjson import loads as json_loads

from porcupine.config.default import DEFAULTS
from porcupine.core.services import get_service
from porcupine.core.router import ContextRouter
from porcupine.apps.schema.users import SystemUser


class PorcupineRequest(Request):
    @property
    def session(self):
        return self.get('session')

    def load_json(self, loads=json_loads):
        return super().load_json(loads)


class PorcupineServer(Sanic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, name='porcupine')
        self.router.server = self
        self.config.update(DEFAULTS)
        self.load_environment_config('PORCUPINE_', preserve_prefix=False)
        self.__identity = SystemUser()

    @property
    def system_user(self):
        return self.__identity

    @property
    def bus(self):
        return get_service('bus').bus

    def cron_tab(self, spec, identity=None):
        if identity is None:
            identity = self.system_user

        scheduler = get_service('scheduler')

        def cron_wrapper(f):
            scheduler.schedule(spec, f, identity)
            return f

        return cron_wrapper

    def migrate(self, f):
        migration_mgr = get_service('migration_mgr')
        migration_mgr.add(f, self.system_user)
        return f

    def load_environment_config(self, prefix, preserve_prefix=True):
        """
        Looks for any prefixed environment variables and applies
        them to the configuration if present.
        """
        for k, v in os.environ.items():
            if k.startswith(prefix):
                if preserve_prefix:
                    config_key = k
                else:
                    _, config_key = k.split(prefix, 1)
                self.config[config_key] = v

    def get_log_config(self, log_to_files: bool) -> dict:
        config = self.config
        log_level = int(config.LOG_LEVEL)
        log_config = {
            'version': 1,
            'formatters': {
                'generic': {
                    'format': config.LOG_FORMAT,
                    'datefmt': config.LOG_DATE_FORMAT
                },
                'access': {
                    'format': config.LOG_ACCESS_FORMAT,
                    'datefmt': config.LOG_DATE_FORMAT
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'generic',
                    'stream': sys.stdout
                },
                'error_console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'generic',
                    'stream': sys.stderr
                },
                'access_console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'access',
                    'stream': sys.stdout
                },
            },
            'loggers': {
                'sanic.root': {
                    'level': log_level,
                    'handlers': ['console']
                },
                'porcupine': {
                    'level': log_level,
                    'handlers': ['console'],
                    'propagate': True,
                    'qualname': 'porcupine'
                },
                'sanic.error': {
                    'level': log_level,
                    'handlers': ['error_console'],
                    'propagate': True,
                    'qualname': 'sanic.error'
                },
                'sanic.access': {
                    'level': log_level,
                    'handlers': ['access_console'],
                    'propagate': True,
                    'qualname': 'sanic.access'
                }
            }
        }

        handlers = log_config['handlers']
        loggers = log_config['loggers']

        if log_to_files:
            rotate_settings = {
                'when': config.LOG_WHEN,
                'interval': int(config.LOG_INTERVAL),
                'backupCount': int(config.LOG_BACKUPS)
            }
            # add rotating file handlers
            handlers['timedRotatingFile'] = {
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'filename': os.path.abspath('porcupine.log'),
                'formatter': 'generic',
                **rotate_settings
            }
            for log in ('sanic.root', 'porcupine', 'sanic.error'):
                loggers[log]['handlers'] = ['timedRotatingFile']
            if config.LOG_ACCESS:
                handlers['accessTimedRotatingFile'] = {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    # 'filters': ['accessFilter'],
                    'filename': os.path.abspath('access.log'),
                    'formatter': 'access',
                    **rotate_settings
                }
                loggers['sanic.access']['handlers'] = \
                    ['accessTimedRotatingFile']

        return log_config


server = PorcupineServer(router=ContextRouter(),
                         request_class=PorcupineRequest,
                         configure_logging=False)
