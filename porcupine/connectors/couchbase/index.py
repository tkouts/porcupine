from porcupine.core.abstract.connector.index import AbstractIndex


class Index(AbstractIndex):
    """
    Couchbase index
    """
    def exists(self, container_id, value):
        pass

    def close(self):
        pass
