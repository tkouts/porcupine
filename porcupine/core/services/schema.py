"""
Schema maintenance service
"""
import asyncio

from porcupine import log
from porcupine.core.services.schematasks.collcompacter import \
    CollectionCompacter
from porcupine.core.services.schematasks.collrebuilder import \
    CollectionReBuilder
from porcupine.core.services.schematasks.schemacleaner import SchemaCleaner
from porcupine.core.services.schematasks.staleremover import StaleRemover

from .service import AbstractService


class SchemaMaintenance(AbstractService):
    service_key = 'schema'
    queue = None

    def start(self, loop):
        type(self).queue = asyncio.Queue()
        asyncio.create_task(self.worker())

    async def worker(self):
        while True:
            task = await self.queue.get()
            if task is None:
                self.queue.task_done()
                break
            try:
                await task.execute()
            except Exception as e:
                log.error(f'Task {type(task).__name__} threw error {str(e)}')
            finally:
                self.queue.task_done()

    def status(self):
        return {
            'queue_size': self.queue.qsize()
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

    async def clean_schema(self, key, ttl):
        if self.queue is not None:
            task = SchemaCleaner(key, ttl)
            await self.queue.put(task)

    async def remove_stale(self, key):
        if self.queue is not None:
            task = StaleRemover(key)
            await self.queue.put(task)
