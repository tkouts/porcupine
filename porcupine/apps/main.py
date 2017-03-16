import logging
import asyncio

from sanic.response import json, HTTPResponse

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


@main.route('/<item_id>', methods=frozenset({'GET', 'POST'}))
async def resource_handler(request, item_id):
    item = await db.get_item(item_id, quiet=False)
    handler = getattr(item, request.method.lower(), None)
    if handler is None:
        raise exceptions.MethodNotAllowed('Method not allowed')
    result = handler(request)
    if asyncio.iscoroutine(result):
        result = await result
    return result if isinstance(result, HTTPResponse) else json(result)


@main.route('/<item_id>/<member>', methods=frozenset({'GET', 'PUT', 'POST'}))
async def member_handler(request, item_id, member):
    item = await db.get_item(item_id, quiet=False)
    if member in item.__schema__ and not item.__schema__[member].protected:
        handler = getattr(item.__schema__[member],
                          request.method.lower(),
                          None)
        if handler is None:
            raise exceptions.MethodNotAllowed('Method not allowed')
        result = handler(request, item)
        if asyncio.iscoroutine(result):
            result = await result
        return result if isinstance(result, HTTPResponse) else json(result)

    raise exceptions.NotFound(
        'The resource {0} does not exist'.format(request.url))
