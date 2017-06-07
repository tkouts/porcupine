import os
import sys
import logging.config

import yaml
from sanic.defaultFilter import DefaultFilter

from .default import default_settings


settings = default_settings
DEBUG = False


def parse(config_file):
    with open(config_file, encoding='utf-8') as f:
        return yaml.load(f.read())


def setup_logging(log_to_files: bool) -> dict:
    log_settings = settings['log']
    log_level = log_settings['level']
    if log_level <= logging.DEBUG:
        global DEBUG
        DEBUG = True
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
                'format': log_settings['format'],
                'datefmt': log_settings['date_format']
            },
            'access': {
                'format': log_settings['access_format'],
                'datefmt': log_settings['date_format']
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

    if log_settings['access_log']:
        # add access logger
        loggers['network'] = {
            'level': log_level,
            'handlers': ['accessStream', 'errorStream']
        }

    if log_to_files:
        rotate_settings = {
            'when': log_settings['when'],
            'interval': log_settings['interval'],
            'backupCount': log_settings['backups']
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

    # load config
    logging.config.dictConfig(log_config)
    return log_config


def add_index(data_type):
    index_map = settings['db']['__indices__']
    # TODO: check duplicates
    index_map[data_type.name] = data_type
