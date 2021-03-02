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
        results = cluster.search_query(
            self.index.container_name,
            query,
            SearchOptions(
                sort=['-_score'] if self._reversed else ['_score']
            )
        )
        async for hit in results:
            context.item_meta[hit.id] = hit.score
            yield hit.id
