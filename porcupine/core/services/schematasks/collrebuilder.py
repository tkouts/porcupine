import asyncio
from inspect import isawaitable
from collections import deque

from porcupine import exceptions
from porcupine.core import utils
from porcupine.core.datatypes.collection import CollectionResolver
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask


class CollectionReBuilder(SchemaMaintenanceTask):
    __slots__ = 'ttl', 'item_id', 'chunk_no', 'collection_name'

    def __init__(self, key, ttl):
        super().__init__(key)
        self.ttl = ttl
        self.item_id, self.collection_name, chunk_no = self.key.split('/')
        self.chunk_no = int(chunk_no)

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

    async def bump_up_active_chunk(self):
        connector = self.connector
        counter_path = utils.get_active_chunk_key(self.collection_name)
        try:
            new_chunk_key = utils.get_collection_key(self.item_id,
                                                     self.collection_name,
                                                     self.chunk_no + 1)
            await connector.insert_multi({new_chunk_key: ''}, ttl=self.ttl)
        except exceptions.DBAlreadyExists:
            pass
        await connector.mutate_in(
            self.item_id,
            {counter_path: (connector.SUB_DOC_UPSERT_MUT, self.chunk_no + 1)}
        )
        if self.ttl:
            await connector.touch_multi({self.item_id: self.ttl})

    async def execute(self):
        # print('splitting collection', self.key)
        connector = self.connector
        # bump up active chunk number
        await self.bump_up_active_chunk()
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
