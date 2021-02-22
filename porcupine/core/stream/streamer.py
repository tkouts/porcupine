from typing import AsyncIterable
from aiostream import stream, streamcontext

from porcupine import pipe
from porcupine.hinting import TYPING
from porcupine import db, exceptions
from porcupine.core.stream.operators import reverse


class BaseStreamer(AsyncIterable):
    def __init__(self, iterator: TYPING.STREAMER_ITERATOR_TYPE):
        self.iterator = iterator
        self.is_wrapped = False

    async def __aiter__(self):
        async with streamcontext(self.iterator) as streamer:
            async for x in streamer:
                yield x

    def __or__(self, p: AsyncIterable):
        iterator = self.iterator
        if not self.is_wrapped:
            iterator = stream.iterate(iterator)
            self.is_wrapped = True
        self.iterator = iterator | p
        return self

    def reverse(self):
        self.iterator = reverse(self.iterator)
        return self

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
            id_iterator |
            pipe.chunks(10) |
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

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'streamer1={self.streamer1}, '
            f'streamer2={self.streamer2})'
        )

    async def _generator(self):
        yield None


class IntersectionStreamer(CombinedIdStreamer):
    async def _generator(self):
        collection = []
        async for sorted_collection in self.streamer1 | pipe.sort():
            collection = sorted_collection
        # print('COLLECTION', collection)
        if collection:
            async for x in self.streamer2:
                if x in collection:
                    yield x


class UnionStreamer(CombinedIdStreamer):
    async def _generator(self):
        collection = []
        async for sorted_collection in self.streamer1 | pipe.sort():
            collection = sorted_collection
        # print('COLLECTION', collection)
        if collection:
            for x in collection:
                yield x
            async for x in self.streamer2:
                if x not in collection:
                    yield x
        else:
            async for x in self.streamer2:
                yield x
