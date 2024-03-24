from enum import Enum
from typing import AsyncIterable, Union

from pypika import Query
from pypika.queries import QueryBuilder, Selectable
from porcupine import pipe
from porcupine.core.schemaregistry import get_content_class
from porcupine.core.context import ctx_user, ctx_db
# from porcupine.core.services import db_connector
from porcupine.core.stream.streamer import ItemStreamer, PartialStreamer
from porcupine.connectors.partial import PartialItem
from porcupine.core.accesscontroller import resolve_visibility


class QueryType(Enum):
    RAW = 0
    PARTIAL = 1
    ITEMS = 2


class Cursor(AsyncIterable):
    def __init__(self, query: QueryBuilder, params):
        self.query = query
        self.params = params

    async def __aiter__(self):
        db = ctx_db.get().db
        statement = self.query.get_sql()
        params = self.params
        positional = params
        if isinstance(params, dict):
            # convert params to positional
            positional = []
            i = 1
            for param, value in params.items():
                statement = statement.replace(f':{param}', f'${i}')
                positional.append(value)
                i += 1
        async with db.transaction():
            cursor = await db.cursor(
                statement,
                *positional
            )
            while True:
                results = await cursor.fetch(200)
                for row in results:
                    yield row
                if len(results) < 200:
                    break


class PorcupineQuery:
    def __init__(
        self,
        query: Union[QueryBuilder, Selectable],
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

    @classmethod
    def from_(
        cls,
        table: Union[Selectable, str],
        query_type=QueryType.ITEMS,
        **kwargs
    ) -> "PorcupineQuery":
        return cls(Query.from_(table, **kwargs), query_type)

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

    def orderby(self, *args, **kwargs):
        q = self._q.orderby(*args, **kwargs)
        return PorcupineQuery(q, self.type, {**self._params})

    def join(self, *args, **kwargs):
        q = self._q.join(*args, **kwargs)
        return PorcupineQuery(q, self.type, {**self._params})

    def on(self, *args, **kwargs):
        q = self._q.on(*args, **kwargs)
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
        db = ctx_db.get()
        # print(self._q.get_sql(), {**kwargs, **self._params})
        results = await db.query(
            self._q.get_sql(),
            {**kwargs, **self._params}
        )
        if self.type in (QueryType.ITEMS, QueryType.PARTIAL):
            user = ctx_user.get()
            results = [
                p for p in (PartialItem(r) for r in results)
                if resolve_visibility(p)
                and await p.can_read(user)
            ]
            if self.type is QueryType.ITEMS:
                loads = db.persist.loads
                results = [loads(p) for p in results]
        if first_only:
            if len(results) == 0:
                return None
            return results[0]
        return results
