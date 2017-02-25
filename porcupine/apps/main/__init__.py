import logging

from sanic.response import json

from porcupine import App
from porcupine import db
from porcupine.config import settings
from porcupine.utils import system


class Porcupine(App):
    db_blueprint = 'db.yml'

    def __init__(self):
        super().__init__('porcupine')
        # self.connector = None
        self.add_route(self.request_handler, '')

    @staticmethod
    async def request_handler(request):
        return json({'a': 1})

    async def before_start(self, app, loop):
        # connect to database
        logging.info('Opening database')
        connector_type = system.get_rto_by_name(settings['db']['type'])
        db.connector = connector_type()
        await db.connector.connect()
        await super().before_start(app, loop)

    async def after_stop(self, app, loop):
        # close database
        await db.connector.close()
