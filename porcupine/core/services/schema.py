"""
Schema maintenance service
"""
import asyncio

from porcupine import log
from porcupine.exceptions import DBAlreadyExists
from porcupine.core.services.schematasks.collcompacter import \
    CollectionCompacter
from porcupine.core.services.schematasks.collrebuilder import \
    CollectionReBuilder
from porcupine.core.services.schematasks.schemacleaner import SchemaCleaner
from porcupine.core.services.schematasks.staleremover import StaleRemover
from porcupine.core.services.schematasks.collcleaner import CollectionCleaner
from porcupine.core.services.schematasks.collautosplitter import \
    CollectionAutoSplitter

from .service import AbstractService


class SchemaMaintenance(AbstractService):
    service_key = 'schema'
    priority = 500

    def __init__(self, server):
        super().__init__(server)
        self.queue = None
        self.collisions = 0

    def start(self, loop):
        self.queue = asyncio.Queue()
        asyncio.create_task(self.worker())

    async def worker(self):
        while True:
            task = await self.queue.get()
            if task is None:
                self.queue.task_done()
                break
            task_key = f'_st_{task.key}'
            try:
                await task.connector.insert_multi({task_key: ''}, ttl=20)
            except DBAlreadyExists:
                log.debug(f'Another schema task for key {task.key} is running')
                self.collisions += 1
                self.queue.task_done()
            else:
                try:
                    await task.execute()
                except Exception as e:
                    log.error(
                        f'Task {type(task).__name__} threw error {str(e)}')
                finally:
                    await task.connector.delete_multi([task_key])
                    self.queue.task_done()

    async def status(self):
        return {
            'queue_size': self.queue.qsize(),
            'key_collisions': self.collisions
        }

    async def stop(self, loop):
        await self.queue.put(None)
        await self.queue.join()

    async def compact_collection(self, key, ttl):
        if self.queue is not None:
            task = CollectionCompacter(key, ttl)
            await self.queue.put(task)

    async def rebuild_collection(self, key, ttl):
        if self.queue is not None:
            task = CollectionReBuilder(key, ttl)
            await self.queue.put(task)

    async def auto_split(self, key, ttl):
        if self.queue is not None:
            task = CollectionAutoSplitter(key, ttl)
            await self.queue.put(task)

    async def clean_schema(self, key, ttl):
        if self.queue is not None:
            task = SchemaCleaner(key, ttl)
            await self.queue.put(task)

    async def remove_stale(self, key):
        if self.queue is not None:
            task = StaleRemover(key)
            await self.queue.put(task)

    async def clean_collection(self, key, stale_ids, ttl):
        # if self.queue is not None:
        task = CollectionCleaner(key, stale_ids, ttl)
        await self.queue.put(task)
