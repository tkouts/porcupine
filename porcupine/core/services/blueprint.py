import inspect

from sanic import Blueprint
from .db import Db
from .schema import SchemaMaintenance
from .sessionmgr import SessionManager


services = (SchemaMaintenance, Db, SessionManager)
services_blueprint = Blueprint('services')


def prepare_services(server):
    for service in services:
        service.prepare(server)
    server.blueprint(services_blueprint)


@services_blueprint.listener('before_server_start')
async def start_services(server, _):
    for service in services:
        starter = service.start(server)
        if inspect.isawaitable(starter):
            await starter


@services_blueprint.listener('after_server_stop')
async def shutdown_services(server, _):
    for service in services:
        stopper = service.stop(server)
        if inspect.isawaitable(stopper):
            await stopper
