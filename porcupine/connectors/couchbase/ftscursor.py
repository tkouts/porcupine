from couchbase.search import ConjunctionQuery, TermQuery, MatchQuery

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
        match_query = MatchQuery(self._term)
        query = ConjunctionQuery(match_query, scope_query)
        results = cluster.search_query(
            self.index.container_name,
            query
        )
        async for hit in results:
            # print(hit.id)
            yield hit.id
