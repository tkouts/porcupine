from porcupine.connectors.base.index import BaseIndex
from porcupine.connectors.couchbase.cursor import Cursor


class FTSIndex(BaseIndex):
    """
    Couchbase FTS index
    """
    def get_definition(self) -> dict:
        return {}

    def get_cursor(self, **options):
        return Cursor(self, **options)

    def close(self):
        ...
