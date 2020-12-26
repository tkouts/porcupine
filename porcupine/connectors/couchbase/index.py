from porcupine.connectors.base.index import BaseIndex
from porcupine.connectors.couchbase.cursor import Cursor


class Index(BaseIndex):
    """
    Couchbase index
    """
    def get_view(self):
        view = {
            'reduce': '_count'
        }
        subclasses = ','.join(
            [f"'{cls.__name__}'" for cls in self.container_types]
        )
        map_func = """
            function(d, m) {{
                if (
                    '_pcc' in d &&
                    [{1}].lastIndexOf(d._pcc) > -1 &&
                    '{0}' in d
                ) {{
                    emit([d.pid, d.{0}]);
                }}
            }}
        """
        # print(str.format(map_func, self.key, subclasses))
        view['map'] = str.format(map_func, self.key, subclasses)
        return view

    def get_cursor(self):
        return Cursor(self)

    def close(self):
        ...
