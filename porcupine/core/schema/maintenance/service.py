"""
Schema maintenance service
"""
import asyncio
import math

from porcupine import log, db


class SchemaMaintenance:
    queue = None

    @classmethod
    def start(cls):
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
    async def stop(cls):
        log.info('Stopping schema maintenance service')
        await cls.queue.put(None)
        await cls.queue.join()

    @classmethod
    async def compact_collection(cls, key):
        task = CollectionCompacter(key)
        await cls.queue.put(task)

    @classmethod
    async def split_collection(cls, key):
        task = CollectionSplitter(key)
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
        return ' '.join(uniques.keys())

    async def execute(self):
        raw_collection, cas = await db.connector.get_for_update(self.key)
        success = await db.connector.check_and_set(
            self.key,
            self.compact_set(raw_collection),
            cas=cas)
        if not success:
            log.info('Failed to compact {0}'.format(self.key))


class CollectionSplitter(SchemaMaintenanceTask):
    @staticmethod
    def split_set(raw_string, parts):
        chunks = []
        collection = [op for op in raw_string.split(' ') if op]
        avg = len(collection) / parts
        last = 0.0
        while last < len(collection):
            chunks.append(collection[int(last):int(last + avg)])
            last += avg
        return [' '.join(chunk) for chunk in chunks]

    async def execute(self):
        # print('splitting collection', self.key)
        item_id, collection_name, chunk_no = self.key.split('/')
        chunk_no = int(chunk_no)
        # compute number of parts
        raw_collection, cas = await db.connector.get_for_update(self.key)
        size = len(raw_collection)
        split_threshold = db.connector.coll_split_threshold
        if size > split_threshold:
            parts = math.ceil(size / split_threshold)
            # bump up active chunk number
            # so that new collection appends are done in a new doc
            await db.connector.bump_up_chunk_number(item_id, collection_name,
                                                    int(parts))
            while True:
                chunks = self.split_set(raw_collection, parts)
                # print('got {} chunks'.format(len(chunks)))
                first = chunks.pop(0)
                success = await db.connector.check_and_set(
                    self.key,
                    first,
                    cas=cas
                )
                if success:
                    break
                else:
                    # pending transactions have modified the collection
                    # wait for pending to complete and try again
                    await asyncio.sleep(0.1)
                    raw_collection, cas = await db.connector.get_for_update(
                        self.key)
            # add other chunks
            chunks = {
                '{0}/{1}/{2}'.format(item_id, collection_name,
                                     chunk_no + i + 1): chunk
                for (i, chunk) in enumerate(chunks)
            }
            await db.connector.write_chunks(chunks)
