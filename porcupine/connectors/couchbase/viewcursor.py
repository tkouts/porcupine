from couchbase_core.views.params import *
from porcupine.connectors.base.cursors import (
    SecondaryIndexCursor,
    SecondaryIndexIterator
)


class Cursor(SecondaryIndexCursor):
    """
    Couchbase view cursor
    """
    async def count(self):
        if not self.is_ranged:
            self.iterator.reduce = True
            async for count in self:
                return count
            self.iterator.reduce = False
            return 0
        return await super().count()

    def get_iterator(self):
        return CursorIterator(
            self.index,
            self.options.get('stale', STALE_UPDATE_AFTER)
        )

    def close(self):
        pass


class CursorIterator(SecondaryIndexIterator):
    """
    Couchbase view iterator
    """
    def __init__(self, index, stale):
        super().__init__(index)
        self.reduce = False
        self.stale = stale

    async def __aiter__(self):
        # print(self.index.name, self.stale)
        kwargs = {
            'stale': self.stale,
            'reduce': self.reduce
        }

        is_ranged = self.is_ranged

        if self._bounds is None:
            kwargs['mapkey_range'] = [
                [self._scope],
                [self._scope, Query.STRING_RANGE_END]
            ]
        elif not is_ranged:
            # equality
            kwargs['key'] = [self._scope, self._bounds]
        else:
            # range
            kwargs['mapkey_range'] = [
                [self._scope, self._bounds.l_bound],
                [self._scope, self._bounds.u_bound or Query.STRING_RANGE_END]
            ]

        exclude_key = None
        if self._reversed:
            kwargs['descending'] = True
            if is_ranged or self._bounds is None:
                kwargs['mapkey_range'].reverse()
                if self._bounds is not None:
                    if not self._bounds.u_inclusive:
                        exclude_key = self._bounds.u_bound
                    if not self._bounds.l_inclusive:
                        kwargs['inclusive_end'] = False
        else:
            if is_ranged:
                if not self._bounds.l_inclusive:
                    exclude_key = self._bounds.l_bound
                if not self._bounds.u_inclusive:
                    kwargs['inclusive_end'] = False

        bucket = self.index.connector.bucket

        # print(kwargs)
        results = bucket.view_query(
            self.index.container_name,
            self.index.name,
            **kwargs
        )
        async for result in results:
            if self.reduce:
                yield result.value
            else:
                if exclude_key is not None and result.key[1] == exclude_key:
                    continue
                # TODO: return new uncommitted items
                yield result.id
