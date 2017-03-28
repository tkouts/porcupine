from sanic import Blueprint
from porcupine import db, log
from porcupine.config import settings
from porcupine.utils import system
from .schema.maintenance.service import SchemaMaintenance


services = Blueprint('services')


async def open_db():
    log.info('Opening database')
    connector_type = system.get_rto_by_name(settings['db']['type'])
    db.connector = connector_type()
    await db.connector.connect()


async def close_db():
    log.info('Closing database')
    await db.connector.close()


@services.listener('before_server_start')
async def init_services(server, loop):
    await open_db()
    await SchemaMaintenance.start()


@services.listener('after_server_stop')
async def shutdown_services(server, loop):
    await close_db()
    await SchemaMaintenance.stop()
