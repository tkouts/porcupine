"""
Scheduler service
"""
import asyncio
from functools import partial, wraps
from dataclasses import make_dataclass

from aiocron import crontab

from porcupine import log, db, server
from porcupine.exceptions import DBAlreadyExists
from porcupine.core.context import with_context, context_user
from porcupine.apps.schema.cronjobs import CronStatus, CronExecution
from porcupine import date
from .service import AbstractService


Task = make_dataclass('Task', ['func', 'spec', 'running'])

spec_aliases = {
    '@yearly': '0 0 1 1 *',
    '@monthly': '0 0 1 * *',
    '@weekly': '0 0 * * 0',
    '@daily': '0 0 * * *',
    '@hourly': '0 * * * *'
}


class Scheduler(AbstractService):
    service_key = 'scheduler'

    @staticmethod
    @db.transactional()
    async def create_cron_status(cron_jobs_container, name, spec):
        cron_status = CronStatus()
        cron_status.name = name
        cron_status.spec = spec
        cron_status.status = 'idle'
        await cron_status.append_to(cron_jobs_container)

    @staticmethod
    @db.transactional()
    async def patch_cron_status(cron_status, **kwargs):
        await cron_status.apply_patch(kwargs)
        await cron_status.update()
        return cron_status

    def __init__(self, srv):
        super().__init__(srv)
        self.__cron_tabs = {}

    @with_context(server.system_user)
    async def start(self, loop):
        cron_jobs = await db.get_item('CRONJOBS')
        for func_id, task in self.__cron_tabs.items():
            cron_job = await cron_jobs.get_child_by_name(func_id)
            if cron_job is None:
                await self.create_cron_status(cron_jobs, func_id, task.spec)
            else:
                await self.patch_cron_status(cron_job, spec=task.spec)
            task.func(loop=loop)

    async def stop(self, loop):
        # wait for running tasks to complete
        while [task for task in self.__cron_tabs.values() if task.running]:
            await asyncio.sleep(1)

    def schedule(self, spec, func, identity):
        func_id = f'{func.__module__}.{func.__qualname__}'
        wrapped = self.ensure_one_instance(func_id, func, identity)
        self.__cron_tabs[func_id] = Task(
            partial(
                crontab,
                spec_aliases.get(spec, spec),
                func=wrapped
            ),
            spec,
            False
        )

    def ensure_one_instance(self, func_id, func, identity):

        @wraps(func)
        @with_context(server.system_user)
        async def scheduled_task():
            started = date.utcnow()
            cron_jobs = await db.get_item('CRONJOBS')
            cron_job = await cron_jobs.get_child_by_name(func_id)
            # if cron_job.status != 'running':
            try:
                cron_execution = CronExecution()
                cron_execution.name = cron_job.name
                cron_execution.started = started
                cron_job = await self.patch_cron_status(
                    cron_job,
                    status='running',
                    running=cron_execution
                )
            except DBAlreadyExists:
                # cron already running
                return

            self.__cron_tabs[func_id].running = True
            async with context_user(identity):
                try:
                    await func()
                except BaseException as e:
                    log.error(
                        f'Uncaught exception in task {func_id} \n{e}'
                    )
                    await self.patch_cron_status(
                        cron_job,
                        status='idle',
                        last_run_status='fail',
                        running=None
                    )
                else:
                    exec_time = (date.utcnow() - started).microseconds / 1000000
                    await self.patch_cron_status(
                        cron_job,
                        status='idle',
                        last_run_status='success',
                        last_successful_run=started,
                        execution_time=exec_time,
                        running=None,
                    )
                finally:
                    self.__cron_tabs[func_id].running = False

        return scheduled_task

    @with_context(server.system_user)
    async def status(self):
        cron_jobs = await db.get_item('CRONJOBS')
        statuses = []
        async for status in cron_jobs.children.items():
            statuses.append(
                status.custom_view(
                    'name',
                    'spec',
                    'status',
                    'last_successful_run',
                    'execution_time'
                )
            )
        return statuses
