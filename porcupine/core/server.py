import os
import sys

from sanic import Sanic
from sanic.request import Request
from sanic.defaultFilter import DefaultFilter

from porcupine.config.default import DEFAULTS
from porcupine.core.router import ContextRouter


class RequestWithSession(Request):
    @property
    def session(self):
        return self.get('session')


class PorcupineServer(Sanic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.router.server = self
        self.config.update(DEFAULTS)
        self.load_environment_config('PORCUPINE_', preserve_prefix=False)

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
            'filters': {
                'accessFilter': {
                    '()': DefaultFilter,
                    'param': [0, 10, 20]
                },
                'errorFilter': {
                    '()': DefaultFilter,
                    'param': [30, 40, 50]
                }
            },
            'formatters': {
                'simple': {
                    'format': config.LOG_FORMAT,
                    'datefmt': config.LOG_DATE_FORMAT
                },
                'access': {
                    'format': config.LOG_ACCESS_FORMAT,
                    'datefmt': config.LOG_DATE_FORMAT
                }
            },
            'handlers': {
                'internal': {
                    'class': 'logging.StreamHandler',
                    'filters': ['accessFilter'],
                    'formatter': 'simple',
                    'stream': sys.stderr
                },
                'accessStream': {
                    'class': 'logging.StreamHandler',
                    'filters': ['accessFilter'],
                    'formatter': 'access',
                    'stream': sys.stderr
                },
                'errorStream': {
                    'class': 'logging.StreamHandler',
                    'filters': ['errorFilter'],
                    'formatter': 'simple',
                    'stream': sys.stderr
                },
            },
            'loggers': {
                'porcupine': {
                    'level': log_level,
                    'handlers': ['internal', 'errorStream']
                },
                'sanic': {
                    'level': log_level,
                    'handlers': ['internal', 'errorStream']
                }
            }
        }

        handlers = log_config['handlers']
        loggers = log_config['loggers']

        if config.LOG_ACCESS_LOG:
            # add access logger
            loggers['network'] = {
                'level': log_level,
                'handlers': ['accessStream', 'errorStream']
            }

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
                'formatter': 'simple',
                **rotate_settings
            }
            for log in ('porcupine', 'sanic'):
                loggers[log]['handlers'] = ['timedRotatingFile']
            if 'network' in loggers:
                handlers['accessTimedRotatingFile'] = {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'filters': ['accessFilter'],
                    'filename': os.path.abspath('access.log'),
                    'formatter': 'access',
                    **rotate_settings
                }
                loggers['network']['handlers'] = ['accessTimedRotatingFile']

        return log_config


server = PorcupineServer(router=ContextRouter(),
                         request_class=RequestWithSession,
                         log_config=None)
