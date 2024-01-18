from enum import Enum
from typing import AsyncIterable
from pypika.queries import QueryBuilder, Query
from porcupine.core.services import db_connector
from porcupine.core.schema.partial import PartialItem
# from porcupine.core.utils.collections import WriteOnceDict


class QueryType(Enum):
    RAW = 0
    PARTIAL = 1
    ITEMS = 2


class Cursor(AsyncIterable):
    def __init__(self, query, params):
        self.query = query
        self.params = params

    async def __aiter__(self):
        results = await db_connector().db.execute(
            self.query.get_sql(),
            self.params
        )
        # print(self.query)
        if self.query.type is QueryType.RAW:
            for row in results:
                yield row
        else:
            for row in results:
                yield PartialItem(row)


class PorcupineQueryBuilder(QueryBuilder):
    def __init__(self, query_type=QueryType.ITEMS, **kwargs):
        super().__init__(**kwargs)
        self.type = query_type

    def cursor(self, params):
        return Cursor(self, params)

    def execute(self, params):
        return db_connector().db.execute(
            self.get_sql(),
            params
        )


class PorcupineQuery(Query):
    @classmethod
    def _builder(cls, **kwargs) -> PorcupineQueryBuilder:
        return PorcupineQueryBuilder(**kwargs)
