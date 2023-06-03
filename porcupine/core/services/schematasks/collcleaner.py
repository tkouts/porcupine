import re
from functools import partial

from porcupine import log
from porcupine.core import utils
from porcupine.connectors.mutations import Formats
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask


class CollectionCleaner(SchemaMaintenanceTask):
    __slots__ = 'ttl', 'stale_ids', 'item_id', 'chunk_no', 'collection_name'

    def __init__(self, key, stale_ids, ttl):
        super().__init__(key)
        self.item_id, self.collection_name, chunk_no = self.key.split('/')
        self.chunk_no = int(chunk_no)
        self.stale_ids = stale_ids
        self.ttl = ttl

    async def fetch_collection_chunk(self, chunk_no):
        chunk_key = utils.get_collection_key(self.item_id,
                                             self.collection_name,
                                             chunk_no)
        return chunk_key, await self.connector.get_raw(chunk_key,
                                                       fmt=Formats.STRING)

    @staticmethod
    async def clean_chunk(chunk, stale_ids):
        cleaned = re.sub(fr'(^|\s)({"|".join(stale_ids)})', '', chunk).strip()
        return cleaned, None

    async def execute(self):
        if self.connector.server.debug:
            log.debug(f'Cleaning collection {self.key}. '
                      f'Removing {len(self.stale_ids)} stale items.')

        connector = self.connector
        chunk_no = self.chunk_no
        reached_last = False
        while True:
            chunk_key, chunk = await self.fetch_collection_chunk(chunk_no)
            if chunk is None:
                reached_last = True
                break
            if self.stale_ids:
                matches = [stale for stale in self.stale_ids if stale in chunk]
                if matches:
                    xform = partial(self.clean_chunk, stale_ids=matches)
                    success, _ = await connector.swap_if_not_modified(
                        chunk_key,
                        xform,
                        Formats.STRING,
                        ttl=self.ttl,
                    )
                self.stale_ids = [stale for stale in self.stale_ids
                                  if stale not in matches]
            if not self.stale_ids:
                # finished
                break
            chunk_no -= 1

        # if is split check if last chunk is blank
        if reached_last and self.chunk_no > 0:
            key, chunk = await self.fetch_collection_chunk(chunk_no + 1)
            if chunk == '':
                await connector.delete(key)
