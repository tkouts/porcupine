import logging

from sanic.response import json

from porcupine import App
from porcupine import db, exceptions
from porcupine.config import settings
from porcupine.utils import system


class Porcupine(App):
    name = 'resources'
    db_blueprint = 'db.yml'

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

main = Porcupine()


@main.route('/<item_id>')
@main.route('/<item_id>/<member>')
async def request_handler(request, item_id, member=None):
    item = await db.connector.get(item_id)
    if item is not None:
        return json(item)
    raise exceptions.NotFound(
        'The resource {0} does not exist'.format(item_id))
