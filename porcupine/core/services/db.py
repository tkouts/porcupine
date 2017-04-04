"""
Database service
"""
from porcupine import log, db
from porcupine.utils import system
from porcupine.config import settings
from .service import AbstractService


class Db(AbstractService):
    @classmethod
    async def start(cls):
        log.info('Opening database')
        connector_type = system.get_rto_by_name(settings['db']['type'])
        db.connector = connector_type()
        await db.connector.connect()

    @classmethod
    async def stop(cls):
        log.info('Closing database')
        await db.connector.close()
