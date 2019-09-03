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

    def reverse(self):
        self.iterator = self.feeder | pipe.reverse()

    def intersection(self, other):
        return IntersectionStreamer(self, other)

    def union(self, other):
        return UnionStreamer(self, other)

    async def count(self):
        n = 0
        async for _ in self:
            n += 1
        return n


class EmptyStreamer(BaseStreamer):
    def __init__(self):
        super().__init__(stream.empty())


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


class CombinedIdStreamer(IdStreamer):
    def __init__(self, streamer1: BaseStreamer, streamer2: BaseStreamer):
        self.streamer1 = streamer1
        if isinstance(streamer1, ItemStreamer):
            streamer1 |= pipe.id_getter
        self.streamer2 = streamer2
        if isinstance(streamer2, ItemStreamer):
            streamer2 |= pipe.id_getter
        super().__init__(self._generator())

    async def _generator(self):
        yield None


class IntersectionStreamer(CombinedIdStreamer):
    async def _generator(self):
        collection = []
        async for sorted_collection in self.streamer1 | pipe.sort():
            collection = sorted_collection
        # print('COLLECTION', collection)
        async for x in self.streamer2:
            if x in collection:
                yield x


class UnionStreamer(CombinedIdStreamer):
    async def _generator(self):
        collection = []
        async for sorted_collection in self.streamer1 | pipe.sort():
            collection = sorted_collection
        # print('COLLECTION', collection)
        for x in collection:
            yield x
        async for x in self.streamer2:
            if x not in collection:
                yield x
