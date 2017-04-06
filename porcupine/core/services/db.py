"""
Database service
"""
from porcupine import log, db
from porcupine.utils import system
from porcupine.config import settings
from .service import AbstractService


class Db(AbstractService):
    @staticmethod
    def get_connector():
        connector_type = system.get_rto_by_name(settings['db']['type'])
        return connector_type()

    @classmethod
    def prepare(cls):
        connector = cls.get_connector()
        connector.prepare_indexes()

    @classmethod
    async def start(cls, server):
        log.info('Opening database')
        db.connector = cls.get_connector()
        await db.connector.connect()

    @classmethod
    async def stop(cls, server):
        log.info('Closing database')
        await db.connector.close()

    @classmethod
    def status(cls):
        return {
            'active_txns': db.connector.active_txns
        }
