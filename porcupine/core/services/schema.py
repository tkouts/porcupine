"""
Schema maintenance service
"""
import asyncio
from porcupine import log, db, exceptions
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
        counter_path = '{0}_'.format(collection_name)
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
            await connector.upsert_multi(chunks)
            # TODO handle exceptions


class SchemaCleaner(SchemaMaintenanceTask):
    @staticmethod
    def schema_updater(item_dict):
        item = db.connector.persist.loads(item_dict)
        item_schema = frozenset([key for key in item_dict.keys()
                                 if not key.startswith('_')
                                 and not key.endswith('_')])
        current_schema = frozenset([dt.storage_key
                                    for dt in item.__schema__.values()])
        # remove old attributes
        for_removal = item_schema.difference(current_schema)
        # composite_pid = ':{}'.format(self.id)
        for attr_name in for_removal:
            # detect if it is composite attribute
            attr_value = item_dict.pop(attr_name)
            # if attr_value:
            #     if isinstance(attr_value, str):
            #         # is it an embedded data type?
            #         item = db._db.get_item(attr_value)
            #         if item is not None and item.parent_id == composite_pid:
            #             Composition._remove_composite(item, True)
            #     elif isinstance(attr_value, list):
            #         # is it a composition data type?
            #         item = db._db.get_item(attr_value[0])
            #         if item is not None and item.parent_id == composite_pid:
            #             items = db._db.get_multi(attr_value)
            #             if all([item.parent_id == composite_pid
            #                     for item in items]):
            #                 for item in items:
            #                     Composition._remove_composite(item, True)
        # update sig
        item_dict['sig'] = type(item).__sig__
        # add cc back
        item_dict['_cc'] = item.content_class
        return item_dict, {}

    async def execute(self):
        # log.info('Cleaning schema of {0}'.format(self.key))
        try:
            success, _ = await db.connector.swap_if_not_modified(
                self.key,
                xform=self.schema_updater
            )
            if not success:
                log.info('Failed to update schema of {0}'.format(self.key))
        except exceptions.NotFound:
            # the key is removed
            pass
