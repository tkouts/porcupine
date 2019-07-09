from porcupine.connectors.base.index import AbstractIndex


class Index(AbstractIndex):
    """
    Couchbase index
    """
    def get_view(self):
        map_func = \
            "function(d, m) {{if ('{0}' in d) {{emit([d.pid, d.{0}]);}}}}"
        return {
            'map': str.format(map_func, self.key),
            'reduce': '_count'
        }

    def exists(self, container_id, value):
        pass

    def close(self):
        ...
