"""
Schema maintenance service
"""
import asyncio
import math
from inspect import isawaitable

from porcupine import log, db, exceptions
from porcupine.core.utils import system
from .service import AbstractService


class SchemaMaintenance(AbstractService):
    queue = None

    @classmethod
    def start(cls, server):
        log.info('Starting schema maintenance service')
        cls.queue = asyncio.Queue()
        asyncio.ensure_future(cls.worker())

    @classmethod
    async def worker(cls):
        while True:
            task = await cls.queue.get()
            if task is None:
                cls.queue.task_done()
                break
            try:
                await task.execute()
            finally:
                cls.queue.task_done()

    @classmethod
    def status(cls):
        return {
            'queue_size': cls.queue.qsize()
        }

    @classmethod
    async def stop(cls, server):
        log.info('Stopping schema maintenance service')
        await cls.queue.put(None)
        await cls.queue.join()

    @classmethod
    async def compact_collection(cls, key):
        task = CollectionCompacter(key)
        await cls.queue.put(task)

    @classmethod
    async def rebuild_collection(cls, key):
        task = CollectionReBuilder(key)
        await cls.queue.put(task)

    @classmethod
    async def clean_schema(cls, key):
        task = SchemaCleaner(key)
        await cls.queue.put(task)


class SchemaMaintenanceTask:
    def __init__(self, key):
        self.key = key


class CollectionCompacter(SchemaMaintenanceTask):
    @staticmethod
    def compact_set(raw_string):
        compacted, _ = system.resolve_set(raw_string)
        return ' '.join(compacted), True

    async def execute(self):
        try:
            success, _ = await db.connector.swap_if_not_modified(
                self.key,
                xform=self.compact_set
            )
            if not success:
                log.info('Failed to compact collection {0}'.format(self.key))
        except exceptions.NotFound:
            # the key is removed
            pass


class CollectionReBuilder(SchemaMaintenanceTask):
    def __init__(self, key):
        super().__init__(key)
        self.item_id, self.collection_name, chunk_no = self.key.split('/')
        self.chunk_no = int(chunk_no)

    async def rebuild_set(self, raw_string):
        # if len(raw_string) < db.connector.coll_split_threshold:
        #     # no op, already split
        #     return None, None
        min_chunk = self.chunk_no
        raw_chunks = [raw_string]
        if self.chunk_no > 0:
            previous_chunks, min_chunk = \
                await system.fetch_collection_chunks(self.key)
            raw_chunks[0:0] = previous_chunks
        collection, _ = system.resolve_set(' '.join(raw_chunks))
        parts = math.ceil(len(' '.join(collection)) /
                          db.connector.coll_split_threshold)
        avg = len(collection) / parts
        chunks = []
        last = 0.0
        while last < len(collection):
            chunks.append(collection[int(last):int(last + avg)])
            last += avg
        raw_chunks = [' '.join(chunk) for chunk in chunks]
        insertions = {
            system.get_collection_key(self.item_id,
                                      self.collection_name,
                                      self.chunk_no - i - 1): chunk
            for i, chunk in enumerate(reversed(raw_chunks[:-1]))
        }
        deletions = []
        unused_chunk = self.chunk_no - len(raw_chunks)
        while unused_chunk >= min_chunk:
            key = system.get_collection_key(self.item_id,
                                            self.collection_name,
                                            unused_chunk)
            deletions.append(key)
            unused_chunk -= 1
        return raw_chunks[-1], (insertions, deletions)

    async def bump_up_active_chunk(self):
        connector = db.connector
        counter_path = system.get_active_chunk_key(self.collection_name)
        try:
            await connector.insert_multi({
                system.get_collection_key(self.item_id, self.collection_name,
                                          self.chunk_no + 1): ''
            })
        except exceptions.DBAlreadyExists:
            pass
        await connector.mutate_in(
            self.item_id,
            {counter_path: (connector.SUB_DOC_UPSERT_MUT, self.chunk_no + 1)}
        )

    async def execute(self):
        # print('splitting collection', self.key)
        connector = db.connector
        # bump up active chunk number
        await self.bump_up_active_chunk()
        # replace active chunk
        while True:
            success, chunks = await connector.swap_if_not_modified(
                self.key,
                xform=self.rebuild_set
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
                task = connector.upsert_multi(insertions)
                if isawaitable(task):
                    tasks.append(asyncio.ensure_future(task))
            if deletions:
                task = connector.delete_multi(deletions)
                if isawaitable(task):
                    tasks.append(asyncio.ensure_future(task))
            if tasks:
                completed, _ = await asyncio.wait(tasks)
                errors = [task.exception() for task in tasks]
                if any(errors):
                    # TODO: log errors
                    print(errors)


class SchemaCleaner(SchemaMaintenanceTask):
    @staticmethod
    def schema_updater(item_dict):
        from porcupine.datatypes import Blob, ReferenceN, RelatorN

        clazz = system.get_content_class(item_dict['_cc'])
        item_schema = frozenset([key for key in item_dict.keys()
                                 if not key.startswith('_')
                                 and not key.endswith('_')])
        current_schema = frozenset([dt.storage_key
                                    for dt in clazz.__schema__.values()])
        for_removal = item_schema.difference(current_schema)
        externals = {}
        # remove old attributes
        for attr_name in for_removal:
            # detect if it is composite attribute
            attr_value = item_dict.pop(attr_name)
            # TODO: handle composites
            if isinstance(attr_value, str):
                if attr_value == Blob.storage_info:
                    externals[attr_name] = (attr_value, None)
                elif attr_value == ReferenceN.storage_info \
                        or attr_value.startswith(RelatorN.storage_info_prefix):
                    try:
                        active_chunk_key = \
                            system.get_active_chunk_key(attr_name)
                        active_chunk = item_dict.pop(active_chunk_key)
                    except KeyError:
                        continue
                    externals[attr_name] = (attr_value, active_chunk)
        # update sig
        item_dict['sig'] = clazz.__sig__
        return item_dict, externals

    async def execute(self):
        from porcupine.datatypes import Blob, ReferenceN, RelatorN

        try:
            success, externals = await db.connector.swap_if_not_modified(
                self.key,
                xform=self.schema_updater
            )
            if not success:
                log.info('Failed to update schema of {0}'.format(self.key))
                return
        except exceptions.NotFound:
            # the key is removed
            return

        external_keys = []
        for ext_name, ext_info in externals.items():
            ext_type, active_chunk = ext_info
            if ext_type == Blob.storage_info:
                external_key = system.get_blob_key(self.key, ext_name)
                if db.connector.exists(external_key):
                    external_keys.append(external_key)
            elif ext_type == ReferenceN.storage_info \
                    or ext_type.startswith(RelatorN.storage_info_prefix):
                external_key = system.get_collection_key(self.key, ext_name,
                                                         active_chunk)
                while (await db.connector.exists(external_key))[1]:
                    external_keys.append(external_key)
                    active_chunk -= 1
                    external_key = system.get_collection_key(
                        self.key, ext_name, active_chunk)

        if external_keys:
            # TODO handle exceptions
            await db.connector.delete_multi(external_keys)
