from couchbase.management.views import DesignDocument


class DesignDocumentWithOptions(DesignDocument):
    def __init__(self, name: str, views: dict, options: dict = None):
        super().__init__(name, views)
        self._options = options or {}

    def as_dict(self, namespace):
        d = super().as_dict(namespace)
        d['options'] = self._options
        return d
