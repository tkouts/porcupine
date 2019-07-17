from porcupine.connectors.base.index import BaseIndex
from porcupine.connectors.couchbase.cursor import Cursor


class Index(BaseIndex):
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

    def get_cursor(self):
        return Cursor(self)

    def close(self):
        ...
