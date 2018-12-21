import inspect
from collections import OrderedDict

from sanic import Blueprint


_services = OrderedDict()
_services_blueprint = Blueprint('services')


def prepare_services(server):
    from .db import Db
    from .schema import SchemaMaintenance
    from .sessionmgr import SessionManager

    for service_type in (SchemaMaintenance, Db, SessionManager):
        service = service_type(server)
        _services[service.service_key] = service

    server.blueprint(_services_blueprint)


def get_service(key):
    if key is None:
        return _services
    return _services[key]


def db_connector():
    return get_service('db').connector


@_services_blueprint.listener('before_server_start')
async def start_services(*_):
    for service in _services.values():
        starter = service.start()
        if inspect.isawaitable(starter):
            await starter


@_services_blueprint.listener('after_server_stop')
async def shutdown_services(*_):
    for service in _services.values():
        stopper = service.stop()
        if inspect.isawaitable(stopper):
            await stopper
