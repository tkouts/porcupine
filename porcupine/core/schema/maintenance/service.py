"""
Schema maintenance service
"""
import asyncio
from porcupine import log, db, exceptions


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
    async def split_collection(cls, key, parts):
        task = CollectionSplitter(key, parts)
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
                print('failed to split')
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
