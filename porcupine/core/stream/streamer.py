from collections import AsyncIterable, AsyncGenerator
from typing import AsyncIterator
from aiostream import stream, pipe

from porcupine.hinting import TYPING
from porcupine import db, exceptions


class Streamer(AsyncIterable):
    def __init__(self, iterator: AsyncGenerator):
        self.iterator = iterator

    def __aiter__(self) -> TYPING.ITEM_ID:
        return self.iterator

    async def count(self):
        n = 0
        async for _ in self:
            n += 1
        return n

    async def items(self,
                    skip=0,
                    take=None,
                    _multi_fetch=db.get_multi) -> AsyncIterator[
                                                  TYPING.ANY_ITEM_CO]:

        feeder = stream.chunks(self, 40) | pipe.flatmap(_multi_fetch,
                                                        task_limit=1)

        if skip > 0:
            feeder |= pipe.skip(skip)
        if take is not None:
            feeder |= pipe.take(take)

        async with feeder.stream() as streamer:
            async for i in streamer:
                if i is not None:
                    yield i

    async def get_item_by_id(self,
                             item_id: TYPING.ITEM_ID,
                             quiet=True) -> TYPING.ANY_ITEM_CO:
        async for oid in self:
            if oid == item_id:
                return await db.get_item(item_id, quiet=quiet)
        if not quiet:
            raise exceptions.NotFound(f'The resource {item_id} does not exist')

    async def has(self, item_id: TYPING.ITEM_ID):
        async for oid in self:
            if oid == item_id:
                return True
        return False
