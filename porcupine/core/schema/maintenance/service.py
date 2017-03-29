"""
Schema maintenance service
"""
import asyncio
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


class CollectionCompacter:
    def __init__(self, key):
        self.key = key

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
        success = await db.connector.replace_atomically(self.key,
                                                        self.compact_set)
        if not success:
            log.info('Failed to compact {0}'.format(self.key))
