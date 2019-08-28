from typing import AsyncIterable
from aiostream import stream

from porcupine import pipe
from porcupine.hinting import TYPING
from porcupine import db, exceptions


class BaseStreamer(AsyncIterable):
    def __init__(self, iterator: AsyncIterable):
        self.iterator = iterator

    @property
    def feeder(self):
        return stream.iterate(self.iterator)

    async def __aiter__(self):
        async with self.feeder.stream() as streamer:
            async for x in streamer:
                yield x

    def __or__(self, p: AsyncIterable):
        self.iterator = self.feeder | p
        return self

    async def count(self):
        n = 0
        async for _ in self:
            n += 1
        return n


class IdStreamer(BaseStreamer):

    def items(self, _multi_fetch=db.get_multi) -> 'ItemStreamer':
        return ItemStreamer(self, _multi_fetch)

    async def get_item_by_id(self,
                             item_id: TYPING.ITEM_ID,
                             quiet=True) -> TYPING.ANY_ITEM_CO:
        async for oid in self:
            if oid == item_id:
                return await db.get_item(item_id, quiet=quiet)
        if not quiet:
            raise exceptions.NotFound(f'The resource {item_id} does not exist')

    async def has(self, item_id: TYPING.ITEM_ID) -> bool:
        async for oid in self:
            if oid == item_id:
                return True
        return False


class ItemStreamer(BaseStreamer):
    def __init__(self, id_iterator: IdStreamer, multi_fetch):
        item_iterator = (
            id_iterator | pipe.chunks(40) |
            pipe.flatmap(multi_fetch, task_limit=1)
        )
        super().__init__(item_iterator)
