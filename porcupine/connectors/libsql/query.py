from enum import Enum
from typing import AsyncIterable
from pypika.queries import QueryBuilder, Query
from porcupine import pipe
from porcupine.core.schemaregistry import get_content_class
from porcupine.core.services import db_connector
from porcupine.core.stream.streamer import ItemStreamer, PartialStreamer
from porcupine.core.schema.partial import PartialItem
from porcupine.core.schema.elastic import Elastic
from porcupine.core.accesscontroller import resolve_visibility


class QueryType(Enum):
    RAW = 0
    RAW_ASSOCIATIVE = 1
    PARTIAL = 2
    ITEMS = 3


class Cursor(AsyncIterable):
    def __init__(self, query, params):
        self.query = query
        self.params = params

    async def __aiter__(self):
        results = await db_connector().query(
            self.query.get_sql(),
            self.params
        )
        for row in results:
            yield row


class PorcupineQueryBuilder(QueryBuilder):
    def __init__(
        self,
        query_type=QueryType.ITEMS,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.type = query_type
        self._params = {}

    @staticmethod
    async def _shortcut_resolver(i):
        shortcut = get_content_class('Shortcut')
        if isinstance(i, shortcut):
            return await i.get_target()
        return i

    def set_params(self, params):
        self._params = params

    def cursor(self, skip=0, take=None, resolve_shortcuts=False, **kwargs):
        cursor = Cursor(self, {
            **kwargs,
            **self._params
        })
        if self.type is QueryType.ITEMS:
            items = ItemStreamer(cursor)
            if resolve_shortcuts:
                items |= pipe.map(self._shortcut_resolver)
                items |= pipe.if_not_none()
            if skip or take:
                items |= pipe.skip_and_take(skip, take)
            return items
        elif self.type is QueryType.PARTIAL:
            # TODO: check resolve_shortcuts is False
            partials = PartialStreamer(cursor)
            if skip or take:
                partials |= pipe.skip_and_take(skip, take)
            return partials
        else:
            # RAW
            return cursor

    async def execute(self, first_only=False, **kwargs):
        results = await db_connector().db.execute(
            self.get_sql(),
            {**kwargs, **self._params}
        )
        if self.type in (QueryType.ITEMS, QueryType.PARTIAL):
            results = [
                p for p in (PartialItem(r) for r in results)
                if await resolve_visibility(p)
            ]
            if self.type is QueryType.ITEMS:
                results = [Elastic.from_partial(p) for p in results]
        if first_only:
            if len(results) == 0:
                return None
            return results[0]
        return results


class PorcupineQuery(Query):
    @classmethod
    def _builder(cls, **kwargs) -> PorcupineQueryBuilder:
        return PorcupineQueryBuilder(**kwargs)
