"""
Database service
"""
from porcupine.core import utils
from .service import AbstractService


class Db(AbstractService):
    service_key = 'db'

    def __init__(self, server):
        super().__init__(server)
        connector_type = utils.get_rto_by_name(self.server.config.DB_IF)
        self.connector = connector_type(self.server)
        self.connector.prepare_indexes()

    async def start(self, loop):
        await self.connector.connect()

    async def stop(self, loop):
        await self.connector.close()

    async def status(self):
        return {
            'active_txns': self.connector.active_txns
        }
