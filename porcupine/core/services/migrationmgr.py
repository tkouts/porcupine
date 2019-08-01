"""
Migration manager service
"""
import asyncio
from functools import wraps

from namedlist import namedlist

from porcupine import log
from porcupine.exceptions import DBAlreadyExists
from porcupine.core.context import with_context
from . import db_connector
from .service import AbstractService


Migration = namedlist('Migration', 'func running')


class MigrationManager(AbstractService):
    service_key = 'migration_mgr'
    priority = 5

    def __init__(self, server):
        super().__init__(server)
        self.__migrations = {}
        self.__connector = db_connector()

    async def start(self, loop):
        for func_id, migration in self.__migrations.items():
            await migration.func()

    async def stop(self, loop):
        # wait for running tasks to complete
        while [migration for migration in self.__migrations.values()
               if migration.running]:
            await asyncio.sleep(1)

    def add(self, func, identity):
        func_id = f'{func.__module__}.{func.__qualname__}'
        wrapped = self.ensure_once(func_id, with_context(identity)(func))
        self.__migrations[func_id] = Migration(wrapped, False)

    def ensure_once(self, func_id, wrapped):

        @wraps(wrapped)
        async def migration_runner():
            db_key = f'_migration_{func_id}'
            try:
                await self.__connector.insert_multi({db_key: 'running'})
            except DBAlreadyExists:
                # already running or run
                # if running wait for migration to complete or fail
                while True:
                    status = await self.__connector.get_raw(db_key)
                    if status == 'running':
                        await asyncio.sleep(1)
                    else:
                        break
                if status == 'completed':
                    log.info(f'Skipping migration "{func_id}"')
                    return
            log.info(f'Running migration "{func_id}"')
            self.__migrations[func_id].running = True
            try:
                await wrapped()
            except BaseException:
                await self.__connector.delete_multi([db_key])
                raise

            self.__migrations[func_id].running = False
            await self.__connector.upsert_multi({db_key: 'completed'})

        return migration_runner

    async def status(self):
        statuses = {}
        for func_id in self.__migrations:
            status = await self.__connector.get_raw(f'_migration_{func_id}')
            statuses[func_id] = status or 'pending'
        return statuses
