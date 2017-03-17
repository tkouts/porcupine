from porcupine.core.abstract.db.index import AbstractIndex
from porcupine.utils import system


class Index(AbstractIndex):
    """
    Couchbase index
    """
    system_attrs = ('is_collection', )
    index_map = {
        '_pid':
            "function(doc, meta) {{"
            "if ('{0}' in doc && doc.{0}.slice(0, 1) != ':'){{"
            "emit(doc.{0});if ('_pids' in doc){{"
            "for (var i=0; i<doc._pids.length; i++)"
            "emit('.' + doc._pids[i])}}}}}}",
        'system/attrs':
            "function(doc, meta) {{"
            "if ('{0}' in doc) {{emit([doc._pid, doc.{0}]);"
            "if ('_pids' in doc){{for (var i=0; i<doc._pids.length; i++)"
            "emit(['.' + doc._pids[i], doc.{0}])}}}}}}",
        'schema/attrs':
            "function(doc, meta) {{"
            "if ('{0}' in doc.bag) {{emit([doc._pid, doc.bag.{0}]);"
            "if ('_pids' in doc){{for (var i=0; i<doc._pids.length; i++)"
            "emit(['.' + doc._pids[i], doc.bag.{0}])}}}}}}"
    }

    # def __init__(self, connector, name, unique):
    #     super().__init__(connector, name, unique)

    def add_view(self, views):
        views['idx_%s' % self.name] = {
            'map': str.format(self.index_map.get(
                self.name,
                self.index_map['system/attrs'] if self.name in self.system_attrs else self.index_map['schema/attrs']),
                self.name),
            'reduce': '_count'
        }

    def exists(self, container_id, value):
        if self.unique:
            key = '{}_{}'.format(
                self.name,
                system.hash_series(value).hexdigest()
            )
            return self.connector.get_atomic(container_id, key)
        else:
            kwargs = {'stale': context.data.get('stale', False),
                      'reduce': False,
                      'limit': 1,
                      'key': [container_id, value]}
            # set stale for next read operations
            context.data['stale'] = STALE_OK
            results = self.connector.bucket.query(
                'indexes',
                'idx_{}'.format(self.name),
                **kwargs
            )
            for result in results:
                return result.docid

    def close(self):
        pass
