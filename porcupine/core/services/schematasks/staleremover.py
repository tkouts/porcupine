from porcupine import db, log
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask
from porcupine.core.context import with_context, system_override


class StaleRemover(SchemaMaintenanceTask):
    @with_context()
    @db.transactional()
    async def execute(self):
        if db.connector.server.debug:
            log.debug('Removing stale item {0}'.format(self.key))
        item = await db.connector.get(self.key)
        if item is not None:
            with system_override():
                await item.remove()
