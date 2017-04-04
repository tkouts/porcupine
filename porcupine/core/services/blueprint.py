import asyncio

from sanic import Blueprint
from .db import Db
from .schema import SchemaMaintenance


services = (Db, SchemaMaintenance)
services_blueprint = Blueprint('services')


@services_blueprint.listener('before_server_start')
async def start_services(server, loop):
    for service in services:
        starter = service.start()
        if asyncio.iscoroutine(starter):
            await starter


@services_blueprint.listener('after_server_stop')
async def shutdown_services(server, loop):
    for service in services:
        stopper = service.stop()
        if asyncio.iscoroutine(stopper):
            await stopper
