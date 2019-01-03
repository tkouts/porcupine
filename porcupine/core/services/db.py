"""
Database service
"""
from multiprocessing import Lock

from porcupine.core import utils
from porcupine.core.app import App
from .service import AbstractService

_DB_BP_LOCK = Lock()


class Db(AbstractService):
    service_key = 'db'

    def __init__(self, server):
        super().__init__(server)
        connector_type = utils.get_rto_by_name(self.server.config.DB_IF)
        self.connector = connector_type(self.server)
        self.connector.prepare_indexes()

    async def start(self, loop):
        await self.connector.connect()
        # allow only one process at a time
        # to install the db blueprints
        lock_acquired = _DB_BP_LOCK.acquire(False)
        if lock_acquired:
            # install apps db blueprints
            apps = [bp for bp in self.server.blueprints.values()
                    if isinstance(bp, App) and bp.db_blueprint]
            for app in apps:
                await app.setup_db_blueprint()
            _DB_BP_LOCK.release()

    async def stop(self, loop):
        await self.connector.close()

    def status(self):
        return {
            'active_txns': self.connector.active_txns
        }
