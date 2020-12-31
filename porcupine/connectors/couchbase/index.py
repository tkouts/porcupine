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
        map_func = """
            function(d, m) {{
                if ('_pcc' in d && {1}) {{
                    try {{
                        emit([d.pid, d.{0}]);
                    }} catch(e) {{}}
                }}
            }}
        """
        if len(self.all_types) == 1:
            type_name = list(self.all_types.keys())[0].__name__
            type_check = f'"{type_name}" == d._pcc'
        else:
            subclasses = ','.join(
                [f"'{cls.__name__}'" for cls in self.all_types]
            )
            type_check = f'[{subclasses}].includes(d._pcc)'
        # print(str.format(map_func, self.key, subclasses))
        view['map'] = str.format(map_func, self.key, type_check)
        return view

    def get_cursor(self, **options):
        return Cursor(self, **options)

    def close(self):
        ...
