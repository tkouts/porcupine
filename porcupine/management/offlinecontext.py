import os
import asyncio
import typing

from porcupine.core.server import server
from porcupine.core.services.db import Db
from porcupine.core.loader import import_all
from porcupine.core.context import with_context
from porcupine.apps.schema.users import SystemUser


def execute(task: typing.Callable):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_exec(task))


async def _exec(task: typing.Callable):
    import_all(os.getcwd())
    await Db.start(server)
    context_task = with_context(SystemUser())(task)
    await context_task()
