from couchbase.exceptions import NotFoundError
from couchbase.views.params import *

from porcupine.connectors.base.cursor import AbstractCursor


class Cursor(AbstractCursor):
    """
    Couchbase cursor
    """
    def __init__(self,
                 connector,
                 index,
                 fetch_mode=1,
                 enforce_permissions=True,
                 resolve_shortcuts=False):
        super().__init__(connector, index, fetch_mode, enforce_permissions,
                         resolve_shortcuts)
        self.reduce = False

    @property
    def size(self):
        try:
            self.reduce = True
            for value in self:
                return value
        finally:
            self.reduce = False

    def __iter__(self):
        # resolve stale value
        stale = context.data.get('stale', False)
        if not self._scope.startswith('.'):
            # it is not a deep query
            # retrieve stale value from connector
            try:
                self.connector.bucket.delete('%s_stale' % self._scope)
            except NotFoundError:
                stale = STALE_OK

        kwargs = {'stale': True if self.reduce else stale,
                  'streaming': True,
                  'reduce': self.reduce}

        if stale is False:
            # set stale for next read operations
            context.data['stale'] = STALE_OK

        if self._value is not None:
            # equality
            kwargs['key'] = [self._scope, self._value]
        else:
            # range
            if self.index.name == '_pid':
                kwargs['key'] = self._scope
            else:
                kwargs['mapkey_range'] = [
                    [self._scope, self._range._lower_value],
                    [self._scope, self._range._upper_value or
                     Query.STRING_RANGE_END]]

        if self.reduce:
            kwargs['group_level'] = 1

        lower_bound = None
        if self._reversed:
            kwargs['mapkey_range'].reverse()
            kwargs['descending'] = True
            if self._range:
                lower_bound = self._range._upper_value
                if not self._range._lower_inclusive:
                    kwargs['inclusive_end'] = False
        else:
            if self._range:
                lower_bound = self._range._lower_value
                if not self._range._upper_inclusive:
                    kwargs['inclusive_end'] = False

        # print kwargs
        docs = RowProcessor(
            self.connector.bucket,
            self.connector.bucket.query('indexes', 'idx_%s' % self.name, **kwargs),
            not self.reduce and self.fetch_mode)

        # get active transaction info
        if context._trans:
            new_items = context._trans.get_scope(self._scope)
        else:
            new_items = {}

        for doc in docs:
            if self.reduce:
                yield doc.value
            else:
                if self._range and not getattr(
                        self._range,
                        '_upper_inclusive' if self._reversed else '_lower_inclusive'):
                    if doc.key[1] == lower_bound:
                        continue
                if self.fetch_mode == 0:
                    if context._trans is not None and doc.docid in context._trans._locks:
                        if self._fetch_updated(doc.docid):
                            yield doc.docid
                        else:
                            continue
                    else:
                        yield doc.docid
                else:
                    if context._trans is not None and doc.docid in context._trans._locks:
                        updated = self._fetch_updated(doc.docid)
                        if updated:
                            yield updated
                        else:
                            continue
                    else:
                        item = self.resolve(doc.doc.value)
                        if item is not None:
                            yield item

        # return new items last
        for k, v in new_items.items():
            if self.fetch_mode == 0:
                yield k
            else:
                item = self.resolve(v.value)
                if item is not None:
                    yield item

    def close(self):
        pass
