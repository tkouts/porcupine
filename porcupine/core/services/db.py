"""
Database service
"""
from multiprocessing import Lock

from porcupine import log, db
from porcupine.core import utils
from porcupine.core.app import App
from .service import AbstractService

DB_BP_LOCK = Lock()


class Db(AbstractService):
    @staticmethod
    def get_connector(server):
        connector_type = utils.get_rto_by_name(server.config.DB_IF)
        return connector_type(server)

    @classmethod
    def prepare(cls, server):
        connector = cls.get_connector(server)
        connector.prepare_indexes()

    @classmethod
    async def start(cls, server):
        log.info('Opening database')
        db.connector = cls.get_connector(server)
        await db.connector.connect()
        # allow only one process at a time
        # to install the db blueprints
        lock_acquired = DB_BP_LOCK.acquire(False)
        if lock_acquired:
            # install apps db blueprints
            apps = [bp for bp in server.blueprints.values()
                    if isinstance(bp, App) and bp.db_blueprint]
            for app in apps:
                await app.setup_db_blueprint()
            DB_BP_LOCK.release()

    @classmethod
    async def stop(cls, server):
        log.info('Closing database')
        await db.connector.close()

    @classmethod
    def status(cls):
        return {
            'active_txns': db.connector.active_txns
        }
