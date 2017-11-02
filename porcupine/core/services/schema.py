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
            except Exception as e:
                log.error('Task {0} threw error {1}'.format(
                    type(task).__name__, str(e)))
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

    @classmethod
    async def remove_stale(cls, key):
        task = StaleRemover(key)
        await cls.queue.put(task)
