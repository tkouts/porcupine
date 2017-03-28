"""
Schema maintenance service
"""
import asyncio
from porcupine import log


class SchemaMaintenance:
    queue = None

    @classmethod
    async def start(cls):
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

    @classmethod
    async def stop(cls):
        log.info('Stopping schema maintenance service')
        await cls.queue.put(None)
        await cls.queue.join()
