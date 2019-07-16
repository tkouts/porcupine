import inspect
from collections import OrderedDict

from sanic import Blueprint

from porcupine import log

_services = OrderedDict()
_services_blueprint = Blueprint('services')


def prepare_services(server):
    from .db import Db
    from .schema import SchemaMaintenance
    from .sessionmgr import SessionManager
    from .scheduler import Scheduler
    from .migrationmgr import MigrationManager
    from .appinstaller import AppInstaller

    for service_type in (Db, SchemaMaintenance, MigrationManager,
                         Scheduler, SessionManager, AppInstaller):
        service = service_type(server)
        _services[service.service_key] = service

    server.blueprint(_services_blueprint)


def get_service(key):
    if key is None:
        return _services
    return _services[key]


def db_connector():
    return _services['db'].connector


@_services_blueprint.listener('before_server_start')
async def start_services(_, loop):
    for service in _services.values():
        log.info(f'Starting {service.service_key} service')
        starter = service.start(loop)
        if inspect.isawaitable(starter):
            await starter


@_services_blueprint.listener('after_server_stop')
async def shutdown_services(_, loop):
    services = list(_services.values())
    services.reverse()
    for service in services:
        log.info(f'Stopping {service.service_key} service')
        stopper = service.stop(loop)
        if inspect.isawaitable(stopper):
            await stopper
