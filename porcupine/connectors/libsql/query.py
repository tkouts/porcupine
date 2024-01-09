from typing import AsyncIterable
from pypika.queries import QueryBuilder, Query


class PorcupineQueryBuilder(QueryBuilder, AsyncIterable):
    async def __aiter__(self):
        yield 1


class PorcupineQuery(Query):
    @classmethod
    def _builder(cls, **kwargs) -> PorcupineQueryBuilder:
        return PorcupineQueryBuilder(**kwargs)
