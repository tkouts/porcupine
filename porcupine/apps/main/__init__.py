import logging
import asyncio

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
    item = await db.get_item(item_id, quiet=False)
    if member is None and request.url.endswith('/') and item.is_collection:
        member = 'children'
    if member is None:
        handler = getattr(item, request.method.lower(), None)
        if handler is None:
            raise exceptions.MethodNotAllowed('Method not allowed')
        result = handler(request)
        if asyncio.iscoroutine(result):
            return json(await result)
        return json(result)
    elif member in item.__schema__ and not item.__schema__[member].protected:
        handler = getattr(item.__schema__[member],
                          request.method.lower(),
                          None)
        if handler is None:
            raise exceptions.MethodNotAllowed('Method not allowed')
        result = handler(request, item)
        if asyncio.iscoroutine(result):
            return json(await result)
        return json(result)

    raise exceptions.NotFound(
        'The resource {0} does not exist'.format(request.url))
