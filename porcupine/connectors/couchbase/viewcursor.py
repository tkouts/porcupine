import json

from couchbase.views import ViewScanConsistency, ViewOrdering
from couchbase.options import ViewOptions

from porcupine.connectors.base.cursors import (
    SecondaryIndexCursor,
    SecondaryIndexIterator
)

STRING_RANGE_END = json.loads('"\u0FFF"')


class Cursor(SecondaryIndexCursor):
    """
    Couchbase view cursor
    """
    async def count(self):
        if self.can_be_reduced():
            self._iterator.reduce = True
            async for count in self:
                return count
            self._iterator.reduce = False
            return 0
        return await super().count()

    def can_be_reduced(self):
        if not self.is_ranged:
            return True
        last_boundary = self.bounds[-1].value
        return (
            last_boundary.u_inclusive if self._reversed
            else last_boundary.l_inclusive
        )

    def get_iterator(self):
        return CursorIterator(
            self.index,
            self.options.get('stale', ViewScanConsistency.UPDATE_AFTER)
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
        view_options = ViewOptions(
            scan_consistency=self.stale,
            reduce=self.reduce,
        )

        bounds_size = self._bounds is not None and len(self._bounds)
        is_ranged = self.is_ranged
        last = self._bounds[-1].value
        if not is_ranged and bounds_size == len(self.index.keys):
            # equality
            view_options['key'] = [self._scope] + [b.value for b in self._bounds]
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
                end_key.append(last.u_bound or STRING_RANGE_END)
            else:
                start_key.append(last)
                end_key.append(last)

            if len(self._bounds) < len(self.index.keys):
                end_key.append(STRING_RANGE_END)

            view_options['startkey'] = start_key
            view_options['endkey'] = end_key

        exclude_key = None
        if self._reversed:
            view_options['order'] = ViewOrdering.DESCENDING
            if 'endkey' in view_options:
                # reverse range keys
                end_key = view_options['endkey']
                view_options['endkey'] = view_options['startkey']
                view_options['startkey'] = end_key
                if is_ranged:
                    if not last.u_inclusive:
                        exclude_key = last.u_bound
                    if not last.l_inclusive:
                        view_options['inclusive_end'] = False
        elif is_ranged:
            if not last.l_inclusive:
                exclude_key = last.l_bound
            if not last.u_inclusive:
                view_options['inclusive_end'] = False

        bucket = self.index.connector.bucket

        # print(view_options, exclude_key)
        results = bucket.view_query(
            self.index.container_name,
            self.index.name,
            view_options
        )
        async for result in results:
            if self.reduce:
                yield result.value
            else:
                if (
                    exclude_key is not None
                    and result.key[bounds_size] == exclude_key
                ):
                    continue
                # TODO: return new uncommitted items
                yield result.id
