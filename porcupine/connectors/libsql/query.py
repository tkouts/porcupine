from enum import Enum
from typing import AsyncIterable

from pypika.queries import QueryBuilder, Query
from porcupine import pipe
from porcupine.core.schemaregistry import get_content_class
from porcupine.core.context import ctx_user
from porcupine.core.services import db_connector
from porcupine.core.stream.streamer import ItemStreamer, PartialStreamer
from porcupine.connectors.partial import PartialItem
from porcupine.core.accesscontroller import resolve_visibility


class QueryType(Enum):
    RAW = 0
    RAW_ASSOCIATIVE = 1
    PARTIAL = 2
    ITEMS = 3


class Cursor(AsyncIterable):
    def __init__(self, query: QueryBuilder, params):
        self.query = query
        self.params = params

    async def __aiter__(self):
        results = await db_connector().query(
            self.query.get_sql(),
            self.params
        )
        for row in results:
            yield row


class PorcupineQuery:
    def __init__(
        self,
        query: QueryBuilder,
        query_type=QueryType.ITEMS,
        params=None
    ):
        self._q = query
        self.type = query_type
        self._params = params or {}

    @staticmethod
    async def _shortcut_resolver(i):
        shortcut = get_content_class('Shortcut')
        if isinstance(i, shortcut):
            return await i.get_target()
        return i

    def set_params(self, params):
        self._params = params

    def __getattr__(self, item):
        return getattr(self._q, item)

    def where(self, criterion):
        q = self._q.where(criterion)
        return PorcupineQuery(q, self.type, {**self._params})

    def select(self, *args, **kwargs):
        q = self._q.select(*args, **kwargs)
        return PorcupineQuery(q, self.type, {**self._params})

    def __mul__(self, other: 'QueryBuilder'):
        q = self._q * other
        return PorcupineQuery(q, self.type, {**self._params})

    def cursor(
        self,
        skip=0,
        take=None,
        resolve_shortcuts=False,
        _skip_acl_check=False,
        **kwargs
    ):
        cursor = Cursor(self._q, {
            **kwargs,
            **self._params
        })
        if self.type is QueryType.ITEMS:
            items = ItemStreamer(cursor, _skip_acl_check)
            if resolve_shortcuts:
                items |= pipe.map(self._shortcut_resolver)
                items |= pipe.if_not_none()
            if skip or take:
                items |= pipe.skip_and_take(skip, take)
            return items
        elif self.type is QueryType.PARTIAL:
            # TODO: check resolve_shortcuts is False
            partials = PartialStreamer(cursor, _skip_acl_check)
            if skip or take:
                partials |= pipe.skip_and_take(skip, take)
            return partials
        else:
            # RAW
            return cursor

    async def execute(self, first_only=False, **kwargs):
        connector = db_connector()
        results = await connector.query(
            self._q.get_sql(),
            {**kwargs, **self._params}
        )
        if self.type in (QueryType.ITEMS, QueryType.PARTIAL):
            user = ctx_user.get()
            results = [
                p for p in (PartialItem(r) for r in results)
                if await resolve_visibility(p)
                and await p.can_read(user)
            ]
            if self.type is QueryType.ITEMS:
                loads = connector.persist.loads
                results = [loads(p) for p in results]
        if first_only:
            if len(results) == 0:
                return None
            return results[0]
        return results
