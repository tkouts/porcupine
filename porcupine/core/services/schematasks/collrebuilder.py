import asyncio
from inspect import isawaitable
from collections import deque

from porcupine import log
from porcupine.core import utils
from porcupine.core.datatypes.collection import CollectionResolver
from porcupine.core.services.schematasks.task import CollectionMaintenanceTask


class CollectionReBuilder(CollectionMaintenanceTask):
    __slots__ = ()

    async def rebuild_set(self, _raw_string):
        # if len(raw_string) < db.connector.coll_split_threshold:
        #     # no op, already split
        #     return None, None
        max_chunk_size = self.connector.coll_split_threshold
        rebuilt_chunks = []
        resolver = CollectionResolver(self.item_id, self.collection_name,
                                      self.chunk_no)
        chunk = deque()
        current_size = 0
        async for item_id in resolver:
            key_size = len(item_id) + 1  # plus one for separator
            if current_size + key_size > max_chunk_size:
                rebuilt_chunks.append(' '.join(chunk))
                chunk.clear()
                current_size = 0
            chunk.appendleft(item_id)
            current_size += key_size

        # add remaining
        if chunk:
            rebuilt_chunks.append(' '.join(chunk))

        first, rest = rebuilt_chunks[0], rebuilt_chunks[1:]

        insertions = {}
        deletions = []
        current_chunk = self.chunk_no - 1

        for chunk in rest:
            chunk_key = utils.get_collection_key(self.item_id,
                                                 self.collection_name,
                                                 current_chunk)
            insertions[chunk_key] = chunk
            current_chunk -= 1

        for i in range(len(resolver.chunk_sizes) - len(rebuilt_chunks)):
            chunk_key = utils.get_collection_key(self.item_id,
                                                 self.collection_name,
                                                 current_chunk)
            deletions.append(chunk_key)
            current_chunk -= 1

        return first, (insertions, deletions)

    async def execute(self):
        connector = self.connector
        # bump up active chunk number
        bumped_up = await self.bump_up_active_chunk()
        if bumped_up:
            if connector.server.debug:
                log.debug(f'Rebuilding collection {self.key}')
            # replace active chunk
            while True:
                success, chunks = await connector.swap_if_not_modified(
                    self.key,
                    xform=self.rebuild_set,
                    ttl=self.ttl
                )
                if success:
                    break
                else:
                    # wait for pending to complete and try again
                    # print('failed to split')
                    await asyncio.sleep(0.1)
            if chunks is not None:
                tasks = []
                insertions, deletions = chunks
                # print(insertions)
                # print(deletions)
                if insertions:
                    task = connector.upsert_multi(insertions, ttl=self.ttl)
                    if isawaitable(task):
                        tasks.append(task)
                if deletions:
                    task = connector.delete_multi(deletions)
                    if isawaitable(task):
                        tasks.append(task)
                if tasks:
                    completed, _ = await asyncio.wait(tasks)
                    errors = [task.exception() for task in tasks]
                    if any(errors):
                        # TODO: log errors
                        print(errors)
