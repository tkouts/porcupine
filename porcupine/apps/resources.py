import asyncio

from sanic.response import json, HTTPResponse

from porcupine import App
from porcupine import db, exceptions
from porcupine.schema import Item, Composite
from porcupine.datatypes import DataType, ReferenceN, Reference1


class Resources(App):
    """Exposes all items over HTTP using a simple REST API"""
    name = 'resources'
    db_blueprint = 'db.yml'

resources = Resources()


@resources.route('/<item_id>',
                 methods=frozenset({'GET', 'PUT', 'PATCH', 'DELETE'}))
async def resource_handler(request, item_id):
    item = await db.get_item(item_id, quiet=False)
    handler = getattr(item, request.method.lower(), None)
    if handler is None:
        raise exceptions.MethodNotAllowed('Method not allowed')
    result = handler(request)
    if asyncio.iscoroutine(result):
        result = await result
    return result if isinstance(result, HTTPResponse) else json(result)


@resources.route('/<item_id>/<path:path>',
                 methods=frozenset({'GET', 'PUT', 'POST'}))
async def member_handler(request, item_id, path):

    async def resolve_path(root, full_path: str):
        request_path = '{0}/{1}'.format(root.id, full_path)
        path_tokens = [x for x in full_path.split('/') if x]
        resolved = inst = root
        while path_tokens:
            attr_name = path_tokens.pop(0)
            if isinstance(resolved, (Item, Composite)):
                inst = resolved
                resolved = getattr(type(inst), attr_name, None)
                if resolved is None:
                    raise exceptions.NotFound(
                        'The resource {0} does not exist'.format(request_path))
                elif isinstance(resolved, Reference1):
                    reference = getattr(inst, resolved.name)
                    resolved = await reference.item(quiet=False)
            elif isinstance(resolved, ReferenceN):
                collection = getattr(inst, resolved.name)
                try:
                    resolved = await collection.get_item_by_id(attr_name,
                                                               quiet=False)
                except exceptions.NotFound:
                    raise exceptions.NotFound(
                        'The resource {0} does not exist'.format(request_path))
            elif isinstance(resolved, DataType):
                raise exceptions.NotFound(
                    'The resource {0} does not exist'.format(request_path))
        return inst, resolved

    item = await db.get_item(item_id, quiet=False)
    instance, member = await resolve_path(item, path)
    # print(instance, member)
    handler = getattr(member, request.method.lower(), None)
    if handler is None:
        raise exceptions.MethodNotAllowed('Method not allowed')
    if isinstance(member, DataType):
        result = handler(instance, request)
    else:
        result = handler(request)
    if asyncio.iscoroutine(result):
        result = await result
    return result if isinstance(result, HTTPResponse) else json(result)
