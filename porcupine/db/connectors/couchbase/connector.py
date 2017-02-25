import random
import couchbase.experimental
from porcupine import context
from porcupine.core.db.connector import AbstractConnector
# from porcupine.exceptions import DBConnectionError
from .transaction import Transaction
from .cursor import Cursor

couchbase.experimental.enable()
from acouchbase.bucket import Bucket


class Couchbase(AbstractConnector):
    TransactionType = Transaction
    CursorType = Cursor

    def __init__(self):
        self.bucket = None

    @property
    def bucket_name(self):
        return self.settings['bucket']

    @property
    def protocol(self):
        return self.settings.get('protocol', 'couchbase')

    @property
    def password(self):
        return self.settings['password']

    async def connect(self):
        hosts = self.settings['hosts'][:]
        random.shuffle(hosts)
        connection_string = '{}://{}/{}'.format(self.protocol,
                                                ','.join(hosts),
                                                self.bucket_name)
        # try:
        self.bucket = Bucket(connection_string,
                             password=self.password)
        await self.bucket.connect()
        # except (CouchbaseTransientError, CouchbaseNetworkError):
        #     raise DBConnectionError

    async def insert_raw(self, key, value):
        context.txn.insert(key, value)

    async def close(self):
        pass
