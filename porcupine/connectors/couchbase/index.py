from couchbase.exceptions import HTTPError
from porcupine.core.abstract.connector.index import AbstractIndex


class Index(AbstractIndex):
    """
    Couchbase index
    """
    def create(self):
        try:
            query = 'CREATE INDEX {0} ON `{1}`({2}) ' \
                    'WITH {{"defer_build": true}};'.format(
                        self.name,
                        self.connector.bucket_name,
                        self.key
                    )
            self.connector.get_query(query).execute()
        except HTTPError as e:
            message = e.objextra.value['errors'][0]['msg'].lower()
            if 'already' not in message:
                raise

    def exists(self, container_id, value):
        pass

    def close(self):
        pass
