from couchbase.search import (
    ConjunctionQuery,
    TermQuery,
    QueryStringQuery,
    SearchOptions
)

from porcupine import context
from porcupine.connectors.base.cursors import FTSIndexCursor, FTSIndexIterator


class FTSCursor(FTSIndexCursor):
    """
    Couchbase FTS cursor
    """
    async def count(self):
        return await super().count()

    def get_iterator(self):
        return FTSCursorIterator(self.index)

    def close(self):
        pass


class FTSCursorIterator(FTSIndexIterator):
    async def __aiter__(self):
        cluster = self.index.connector.cluster
        scope_query = TermQuery(self.scope, field='pid')
        match_query = QueryStringQuery(self._term)
        query = ConjunctionQuery(match_query, scope_query)
        chunk_size = 20
        skip = 0
        options = SearchOptions(
            sort=['-_score'] if self._reversed else ['_score'],
            limit=chunk_size
        )
        returned = chunk_size
        while returned == chunk_size:
            results = cluster.search_query(
                self.index.container_name,
                query,
                options,
                skip=skip
            )
            returned = 0
            async for hit in results:
                returned += 1
                print(hit.id)
                context.item_meta[hit.id] = hit.score
                yield hit.id
            skip += chunk_size
