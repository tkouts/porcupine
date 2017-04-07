from porcupine.core.abstract.connector.index import AbstractIndex


class Index(AbstractIndex):
    """
    Couchbase index
    """
    def create(self):
        mgr = self.connector.bucket.bucket_manager()
        mgr.create_n1ql_index(self.name, fields=[self.key],
                              defer=True,
                              ignore_exists=True)

    def exists(self, container_id, value):
        pass

    def close(self):
        pass
