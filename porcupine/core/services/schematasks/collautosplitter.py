from porcupine import log
from porcupine.core.services.schematasks.task import CollectionMaintenanceTask
from porcupine.connectors.mutations import Formats


class CollectionAutoSplitter(CollectionMaintenanceTask):
    __slots__ = ()

    async def execute(self):
        connector = self.connector
        chunk = await connector.get_raw(self.key, fmt=Formats.STRING)
        if chunk and len(chunk) > connector.coll_split_threshold * 0.8:
            if connector.server.debug:
                log.debug(f'Auto splitting collection {self.key}')
            await self.bump_up_active_chunk()
