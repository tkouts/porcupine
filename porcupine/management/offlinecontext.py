import os
import asyncio
import typing

from porcupine.core.server import server
from porcupine.core import services
from porcupine.core.loader import import_all
from porcupine.core.context import with_context
from porcupine.apps.schema.users import SystemUser


def execute(task: typing.Callable):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_exec(task, loop))


async def _exec(task: typing.Callable, loop):
    services.prepare_services(server)
    import_all(os.getcwd())
    db, schema = services.get_service('db'), services.get_service('schema')
    await db.start(loop)
    schema.start(loop)
    context_task = with_context(SystemUser())(task)
    await context_task()
    await schema.stop(loop)
    await db.stop(loop)
