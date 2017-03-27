import asyncio

from sanic.response import json, HTTPResponse

from porcupine import App
from porcupine import db, exceptions


class Resources(App):
    """Exposes all items over HTTP using a simple REST API"""
    name = 'resources'
    db_blueprint = 'db.yml'

resources = Resources()


@resources.route('/<item_id>',
                 methods=frozenset({'GET', 'POST'}))
async def resource_handler(request, item_id):
    item = await db.get_item(item_id, quiet=False)
    handler = getattr(item, request.method.lower(), None)
    if handler is None:
        raise exceptions.MethodNotAllowed('Method not allowed')
    result = handler(request)
    if asyncio.iscoroutine(result):
        result = await result
    return result if isinstance(result, HTTPResponse) else json(result)


@resources.route('/<item_id>/<member>',
                 methods=frozenset({'GET', 'PUT', 'POST'}))
async def member_handler(request, item_id, member):
    item = await db.get_item(item_id, quiet=False)
    if hasattr(item.__class__, member):
        handler = getattr(getattr(item.__class__, member),
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
