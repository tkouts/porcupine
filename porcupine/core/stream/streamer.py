from typing import Callable, AsyncIterable, Awaitable

from aiostream import stream, async_

from porcupine import pipe
from porcupine.hinting import TYPING
from porcupine.core.context import ctx_user, ctx_db
from porcupine.core.services import db_connector
from porcupine.connectors.partial import PartialItem
from porcupine.core.accesscontroller import resolve_visibility


class BaseStreamer(AsyncIterable):
    def __init__(self, iterator: TYPING.STREAMER_ITERATOR_TYPE):
        self._iterator = iterator
        self._operators = []
        # self._reversed = False

    async def __aiter__(self):
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
        return streamer


class EmptyStreamer(BaseStreamer):
    def __init__(self):
        super().__init__(stream.empty())

    def __repr__(self):
        return f'{self.__class__.__name__}()'

    # async def is_empty(self):
    #     return True


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
    def __init__(self, cursor, _skip_acl_check=False):
        super().__init__(cursor, _skip_acl_check)
        self._operators.append(pipe.map(ctx_db.get().persist.loads))
