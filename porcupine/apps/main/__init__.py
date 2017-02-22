import logging

from sanic.response import json

from porcupine import App
from porcupine.config import settings
from porcupine.utils import system


class Porcupine(App):
    def __init__(self):
        super().__init__('porcupine')
        self.connector = None
        self.add_route(self.request_handler, '')

    @staticmethod
    async def request_handler(request):
        return json({'a': 1})

    @staticmethod
    async def before_start(app, loop):
        # connect to database
        logging.info('Opening database')
        connector_type = system.get_rto_by_name(settings['db']['type'])
        app.connector = connector_type()
        await app.connector.connect()

    @staticmethod
    async def after_stop(app, loop):
        # close database
        await app.connector.close()
