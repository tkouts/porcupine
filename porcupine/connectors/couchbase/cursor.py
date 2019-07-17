# from couchbase.exceptions import NotFoundError
from couchbase.views.params import *

from porcupine.connectors.base.cursor import BaseCursor, AbstractCursorIterator


class Cursor(BaseCursor):
    """
    Couchbase cursor
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
        return CursorIterator(self.index)

    def close(self):
        pass


class CursorIterator(AbstractCursorIterator):
    def __init__(self, index):
        super().__init__(index)
        self.reduce = False

    async def __aiter__(self):
        kwargs = {'stale': STALE_OK,
                  'streaming': True,
                  'reduce': self.reduce}

        is_ranged = self.is_ranged

        if self.reduce:
            kwargs['group_level'] = 1
            # range
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
                [self._scope, self._bounds.u_bound or
                 Query.STRING_RANGE_END]
            ]


        lower_bound = None
        if self._reversed:
            kwargs['mapkey_range'].reverse()
            kwargs['descending'] = True
            if is_ranged:
                lower_bound = self._bounds.u_bound
                if not self._bounds.l_inclusive:
                    kwargs['inclusive_end'] = False
        else:
            if is_ranged:
                lower_bound = self._bounds.l_bound
                if not self._bounds.u_inclusive:
                    kwargs['inclusive_end'] = False

        bucket = self.index.connector.bucket

        # print(kwargs)

        async for result in bucket.query('indexes', self.index.name, **kwargs):
            if self.reduce:
                yield result.value
            else:
                yield result.docid
                # if self._range and not getattr(
                #         self._range,
                #         '_upper_inclusive' if self._reversed else '_lower_inclusive'):
                #     if doc.key[1] == lower_bound:
                #         continue
                # if self.fetch_mode == 0:
                #     if context._trans is not None and doc.docid in context._trans._locks:
                #         if self._fetch_updated(doc.docid):
                #             yield doc.docid
                #         else:
                #             continue
                #     else:
                #         yield doc.docid
                # else:
                #     if context._trans is not None and doc.docid in context._trans._locks:
                #         updated = self._fetch_updated(doc.docid)
                #         if updated:
                #             yield updated
                #         else:
                #             continue
                #     else:
                #         item = self.resolve(doc.doc.value)
                #         if item is not None:
                #             yield item

        # return new items last
        # for k, v in new_items.items():
        #     if self.fetch_mode == 0:
        #         yield k
        #     else:
        #         item = self.resolve(v.value)
        #         if item is not None:
        #             yield item
