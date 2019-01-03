"""
Scheduler service
"""
import asyncio
from functools import partial, wraps

from namedlist import namedlist
from aiocron import crontab

from porcupine.exceptions import DBAlreadyExists
from porcupine.core.context import with_context
from . import db_connector
from .service import AbstractService


Task = namedlist('Task', 'func running')


class Scheduler(AbstractService):
    service_key = 'scheduler'

    def __init__(self, server):
        super().__init__(server)
        self.__cron_tabs = {}
        self.__connector = db_connector()

    def start(self, loop):
        for func_id, task in self.__cron_tabs.items():
            task.func(loop=loop)

    async def stop(self, loop):
        # wait for running tasks to complete
        while [task for task in self.__cron_tabs.values() if task.running]:
            await asyncio.sleep(1)

    def schedule(self, spec, func, identity):
        func_id = f'{func.__module__}.{func.__qualname__}'
        wrapped = self.ensure_one_instance(func_id,
                                           with_context(identity)(func))
        self.__cron_tabs[func_id] = Task(
            partial(crontab, spec, func=wrapped),
            False
        )

    def ensure_one_instance(self, func_id, wrapped):

        @wraps(wrapped)
        async def scheduled_task():
            db_key = f'_task_{func_id}'
            try:
                await self.__connector.insert_multi({db_key: ''})
            except DBAlreadyExists:
                # already running by another process
                return
            self.__cron_tabs[func_id].running = True
            try:
                await wrapped()
            finally:
                self.__cron_tabs[func_id].running = False
                await self.__connector.delete_multi([db_key])

        return scheduled_task

    def status(self):
        return {
            func_id: task.running
            for func_id, task in self.__cron_tabs.items()
        }
