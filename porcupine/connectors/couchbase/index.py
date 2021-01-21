from couchbase.management.views import View

from porcupine.connectors.base.index import BaseIndex
from porcupine.connectors.couchbase.cursor import Cursor


class Index(BaseIndex):
    """
    Couchbase index
    """
    def get_view(self) -> View:
        map_func = """
            function(d, m) {{
                if (m.type == "json" && '_pcc' in d && !d.dl && {1}) {{
                    try {{
                        emit([d.pid, {0}]);
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
        formatted_keys = [
            f'd.{key}' for key in self.keys
        ]
        return View(
            str.format(map_func, ', '.join(formatted_keys), type_check),
            reduce='_count'
        )

    def get_cursor(self, **options):
        return Cursor(self, **options)

    def close(self):
        ...
