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
            self._iterator.reduce = True
            async for count in self:
                return count
            self._iterator.reduce = False
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
        kwargs = {
            'stale': self.stale,
            'reduce': self.reduce
        }

        bounds_size = self._bounds is not None and len(self._bounds)
        is_ranged = self.is_ranged
        if not is_ranged and bounds_size == len(self.index.keys):
            # equality
            kwargs['key'] = [self._scope] + self._bounds
        else:
            # range
            start_key = [self._scope]
            end_key = [self._scope]
            start_key.extend(self._bounds[:-1])
            end_key.extend(self._bounds[:-1])
            last = self._bounds[-1]

            if is_ranged:
                if last.l_bound is not None:
                    start_key.append(last.l_bound)
                end_key.append(last.u_bound or Query.STRING_RANGE_END)
            else:
                start_key.append(last)
                end_key.append(last)

            if len(self._bounds) < len(self.index.keys):
                end_key.append(Query.STRING_RANGE_END)

            kwargs['mapkey_range'] = [start_key, end_key]

        exclude_key = None
        if self._reversed:
            kwargs['descending'] = True
            if 'mapkey_range' in kwargs:
                kwargs['mapkey_range'].reverse()
                if is_ranged:
                    if not self._bounds[-1].u_inclusive:
                        exclude_key = self._bounds[-1].u_bound
                    if not self._bounds[-1].l_inclusive:
                        kwargs['inclusive_end'] = False
        elif is_ranged:
            if not self._bounds[-1].l_inclusive:
                exclude_key = self._bounds[-1].l_bound
            if not self._bounds[-1].u_inclusive:
                kwargs['inclusive_end'] = False

        bucket = self.index.connector.bucket

        # print(kwargs, exclude_key)
        results = bucket.view_query(
            self.index.container_name,
            self.index.name,
            **kwargs
        )
        async for result in results:
            if self.reduce:
                yield result.value
            else:
                if exclude_key is not None \
                        and result.key[bounds_size] == exclude_key:
                    continue
                # TODO: return new uncommitted items
                yield result.id
