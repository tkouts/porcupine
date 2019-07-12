import asyncio
import math
from inspect import isawaitable

from porcupine import exceptions
from porcupine.core import utils
from porcupine.core.services.schematasks.collcompacter import \
    CollectionCompacter


class CollectionReBuilder(CollectionCompacter):
    __slots__ = 'item_id', 'chunk_no', 'collection_name'

    def __init__(self, key, ttl):
        super().__init__(key, ttl)
        self.item_id, self.collection_name, chunk_no = self.key.split('/')
        self.chunk_no = int(chunk_no)

    async def fetch_collection_chunks(self) -> (list, int):
        prev_chunks = []
        previous_chunk_no = self.chunk_no - 1
        # fetch previous chunks
        while True:
            previous_chunk_key = utils.get_collection_key(self.item_id,
                                                          self.collection_name,
                                                          previous_chunk_no)
            previous_chunk = await self.connector.get_raw(previous_chunk_key)
            if previous_chunk is not None:
                # print(len(previous_chunk))
                prev_chunks.insert(0, previous_chunk)
                previous_chunk_no -= 1
            else:
                break
        return prev_chunks, previous_chunk_no + 1

    async def rebuild_set(self, raw_string):
        # if len(raw_string) < db.connector.coll_split_threshold:
        #     # no op, already split
        #     return None, None
        min_chunk = self.chunk_no
        raw_chunks = [raw_string]
        if self.chunk_no > 0:
            previous_chunks, min_chunk = await self.fetch_collection_chunks()
            raw_chunks[0:0] = previous_chunks
        collection = self.resolve_set(' '.join(raw_chunks))
        if collection:
            parts = math.ceil(len(' '.join(collection)) /
                              self.connector.coll_split_threshold)
            avg = len(collection) / parts
            chunks = []
            last = 0.0
            while last < len(collection):
                chunks.append(collection[int(last):int(last + avg)])
                last += avg
            raw_chunks = [' '.join(chunk) for chunk in chunks]
            insertions = {
                utils.get_collection_key(self.item_id,
                                         self.collection_name,
                                         self.chunk_no - i - 1): chunk
                for i, chunk in enumerate(reversed(raw_chunks[:-1]))
            }
            deletions = []
            unused_chunk = self.chunk_no - len(raw_chunks)
            while unused_chunk >= min_chunk:
                key = utils.get_collection_key(self.item_id,
                                               self.collection_name,
                                               unused_chunk)
                deletions.append(key)
                unused_chunk -= 1
        else:
            # empty collection
            insertions = []
            deletions = [utils.get_collection_key(self.item_id,
                                                  self.collection_name,
                                                  i)
                         for i in range(min_chunk, self.chunk_no + 1)]
        return raw_chunks[-1], (insertions, deletions)

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
                    tasks.append(asyncio.create_task(task))
            if deletions:
                task = connector.delete_multi(deletions)
                if isawaitable(task):
                    tasks.append(asyncio.create_task(task))
            if tasks:
                completed, _ = await asyncio.wait(tasks)
                errors = [task.exception() for task in tasks]
                if any(errors):
                    # TODO: log errors
                    print(errors)
