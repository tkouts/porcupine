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
        last = self._bounds[-1].value
        if not is_ranged and bounds_size == len(self.index.keys):
            # equality
            kwargs['key'] = [self._scope] + [b.value for b in self._bounds]
        else:
            # range
            start_key = [self._scope]
            end_key = [self._scope]
            fixed_values = [b.value for b in self._bounds[:-1]]
            start_key.extend(fixed_values)
            end_key.extend(fixed_values)

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
                    if not last.u_inclusive:
                        exclude_key = last.u_bound
                    if not last.l_inclusive:
                        kwargs['inclusive_end'] = False
        elif is_ranged:
            if not last.l_inclusive:
                exclude_key = last.l_bound
            if not last.u_inclusive:
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
