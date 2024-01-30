from typing import Callable, AsyncIterable, Awaitable
# from functools import partial
from aiostream import stream, async_

from porcupine import pipe
from porcupine.hinting import TYPING
# from porcupine.core.services import db_connector, get_service
from porcupine.core.context import ctx_user
from porcupine.core.schema.elastic import Elastic
from porcupine.core.schema.partial import PartialItem
from porcupine.core.accesscontroller import resolve_visibility
# from porcupine.core.utils.db import (
#     resolve_visibility,
#     is_consistent,
#     get_with_id,
# )


class BaseStreamer(AsyncIterable):
    # supports_reversed_iteration = False
    # output_ids = True

    def __init__(self, iterator: TYPING.STREAMER_ITERATOR_TYPE):
        self._iterator = iterator
        self._operators = []
        # self._reversed = False

    async def __aiter__(self):
        # return self.get_streamer().stream().__aiter__()
        async with self.get_streamer().stream() as streamer:
            async for x in streamer:
                yield x

    def __or__(self, p: Callable[[], AsyncIterable]):
        self._operators.append(p)
        return self

    def list(self) -> Awaitable[list]:
        return stream.list(self)

    def get_streamer(self, _reverse=True):
        streamer = stream.iterate(self._iterator)
        for op in self._operators:
            streamer |= op
        # if _reverse and not self.supports_reversed_iteration
        # and self._reversed:
        #     streamer |= pipe.reverse()
        return streamer

    # def reverse(self):
    #     self._reversed = True
    #     return self

    # def intersection(self, other):
    #     return IntersectionStreamer(self, other)
    #
    # def union(self, other):
    #     return UnionStreamer(self, other)

    # async def count(self) -> int:
    #     streamer = self.get_streamer(_reverse=False)
    #     streamer |= pipe.count()
    #     return await streamer
    #
    # async def is_empty(self) -> bool:
    #     streamer = self.get_streamer(_reverse=False)[0]
    #     try:
    #         await streamer
    #         return False
    #     except IndexError:
    #         return True
    #
    # def items(self, coll=None) -> 'ItemStreamer':
    #     return ItemStreamer(self, coll)


class EmptyStreamer(BaseStreamer):
    def __init__(self):
        super().__init__(stream.empty())

    def __repr__(self):
        return f'{self.__class__.__name__}()'

    async def is_empty(self):
        return True


# class IdStreamer(BaseStreamer):
#     async def get_item_by_id(self,
#                              item_id: TYPING.ITEM_ID,
#                              quiet=True) -> TYPING.ANY_ITEM_CO:
#         has_item_id = await self.has(item_id)
#         if has_item_id:
#             return await db.get_item(item_id, quiet=quiet)
#         if not quiet:
#             raise exceptions.NotFound(f'The resource {item_id}
#             does not exist')
#
#     async def has(self, item_id: TYPING.ITEM_ID) -> bool:
#         has_item = (
#             self.get_streamer(_reverse=False)
#             | pipe.until(lambda x: x == item_id)
#         )
#         try:
#             return await has_item == item_id
#         except StreamEmpty:
#             return False


class PartialStreamer(BaseStreamer):
    def __init__(self, cursor, _skip_acl_check=False):
        super().__init__(cursor)
        self._operators.append(pipe.map(PartialItem))
        self._operators.append(pipe.filter(resolve_visibility))
        if not _skip_acl_check:
            self._operators.append(pipe.filter(
                async_(lambda x: x.can_read(ctx_user.get()))
            ))


class ItemStreamer(PartialStreamer):
    # supports_reversed_iteration = True
    # output_ids = False

    def __init__(self, cursor, _skip_acl_check=False):
        super().__init__(cursor, _skip_acl_check)
        self._operators.append(pipe.map(Elastic.from_partial))

    # def reverse(self):
    #     self._iterator.reverse()
    #     return super().reverse()


# class CombinedIdStreamer(IdStreamer):
#     def __init__(self, streamer1: BaseStreamer, streamer2: BaseStreamer):
#         self.streamer1 = streamer1
#         if not streamer1.output_ids:
#             streamer1 |= pipe.id_getter()
#         self.streamer2 = streamer2
#         if not streamer2.output_ids:
#             streamer2 |= pipe.id_getter()
#         super().__init__(self._generator())
#
#     def __repr__(self):
#         return (
#             f'{self.__class__.__name__}('
#             f'streamer1={self.streamer1}, '
#             f'streamer2={self.streamer2})'
#         )
#
#     async def _generator(self):
#         yield


# class IntersectionStreamer(CombinedIdStreamer):
#     async def _generator(self):
#         stream1 = self.streamer1.get_streamer() | pipe.sort()
#         collection = await stream1
#         # print('COLLECTION', collection)
#         if collection:
#             async for x in self.streamer2:
#                 if x in collection:
#                     yield x


# class UnionStreamer(CombinedIdStreamer):
#     async def _generator(self):
#         stream1 = self.streamer1.get_streamer() | pipe.sort()
#         collection = await stream1
#         # print('COLLECTION', collection)
#         if collection:
#             for x in collection:
#                 yield x
#             async for x in self.streamer2:
#                 if x not in collection:
#                     yield x
#         else:
#             async for x in self.streamer2:
#                 yield x
