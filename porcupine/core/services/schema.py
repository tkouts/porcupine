"""
Schema maintenance service
"""
import asyncio
from porcupine import log, db, exceptions
from porcupine.utils import system
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
    async def split_collection(cls, key, parts):
        task = CollectionSplitter(key, parts)
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
        uniques = {}
        for oid in raw_string.split(' '):
            if oid:
                if oid.startswith('-'):
                    key = oid[1:]
                    if key in uniques:
                        del uniques[key]
                    else:
                        uniques[oid] = None
                else:
                    removal_key = '-{0}'.format(oid)
                    if removal_key in uniques:
                        del uniques[removal_key]
                    uniques[oid] = None
        return ' '.join(uniques.keys()), True

    async def execute(self):
        try:
            success, _ = await db.connector.swap_if_not_modified(
                self.key,
                xform=self.compact_set
            )
            if not success:
                log.info('Failed to compact {0}'.format(self.key))
        except exceptions.NotFound:
            # the key is removed
            pass


class CollectionSplitter(SchemaMaintenanceTask):
    def __init__(self, key, parts):
        super().__init__(key)
        self.parts = parts

    def split_set(self, raw_string):
        if len(raw_string) < db.connector.coll_split_threshold:
            # no op, already split
            return None, None
        chunks = []
        collection = [op for op in raw_string.split(' ') if op]
        avg = len(collection) / self.parts
        last = 0.0
        while last < len(collection):
            chunks.append(collection[int(last):int(last + avg)])
            last += avg
        raw_chunks = [' '.join(chunk) for chunk in chunks]
        return raw_chunks[0], raw_chunks[1:]

    async def execute(self):
        # print('splitting collection', self.key)
        item_id, collection_name, chunk_no = self.key.split('/')
        chunk_no = int(chunk_no)
        counter_path = system.get_active_chunk_key(collection_name)
        connector = db.connector

        # bump up active chunk number
        await connector.mutate_in(item_id, {
            counter_path: (connector.SUB_DOC_UPSERT_MUT,
                           chunk_no + int(self.parts))
        })

        # replace active chunk
        while True:
            success, chunks = await connector.swap_if_not_modified(
                self.key,
                xform=self.split_set
            )
            if success:
                break
            else:
                # wait for pending to complete and try again
                # print('failed to split')
                await asyncio.sleep(0.1)
        if chunks is not None:
            # add other chunks
            chunks = {
                '{0}/{1}/{2}'.format(item_id, collection_name,
                                     chunk_no + i + 1): chunk
                for (i, chunk) in enumerate(chunks)
            }
            # TODO handle exceptions
            await connector.upsert_multi(chunks)


class SchemaCleaner(SchemaMaintenanceTask):
    @staticmethod
    def schema_updater(item_dict):
        from porcupine.datatypes import Blob, ReferenceN, RelatorN

        item = db.connector.persist.loads(item_dict)
        item_schema = frozenset([key for key in item_dict.keys()
                                 if not key.startswith('_')
                                 and not key.endswith('_')])
        current_schema = frozenset([dt.storage_key
                                    for dt in item.__schema__.values()])
        for_removal = item_schema.difference(current_schema)
        externals = {}
        # remove old attributes
        for attr_name in for_removal:
            # detect if it is composite attribute
            attr_value = item_dict.pop(attr_name)
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
        item_dict['sig'] = type(item).__sig__
        # add cc back
        item_dict['_cc'] = item.content_class
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
                while db.connector.exists(external_key):
                    external_keys.append(external_key)
                    active_chunk -= 1
                    external_key = system.get_collection_key(
                        self.key, ext_name, active_chunk)

        if external_keys:
            # TODO handle exceptions
            await db.connector.delete_multi(external_keys)
