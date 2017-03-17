import ujson

import couchbase.experimental
import random
from couchbase.exceptions import NotFoundError, DocumentNotJsonError, \
    SubdocPathNotFoundError

from porcupine import exceptions
from porcupine.core.abstract.db.connector import AbstractConnector
from .cursor import Cursor
from .transaction import Transaction

couchbase.experimental.enable()
from acouchbase.bucket import Bucket


couchbase.set_json_converters(ujson.dumps, ujson.loads)


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
        self.bucket = Bucket(connection_string,
                             password=self.password)
        await self.bucket.connect()

    async def exists(self, key):
        try:
            await self.bucket.retrieve_in(key, 'sys')
        except NotFoundError:
            return False
        except (DocumentNotJsonError, SubdocPathNotFoundError):
            pass
        return True

    async def get_raw(self, key, quiet=True):
        try:
            result = await self.bucket.get(key, quiet=quiet)
        except NotFoundError:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(key))
        return result.value

    async def get_multi_raw(self, keys):
        multi = await self.bucket.get_multi(keys, quiet=True)
        return [multi[key].value for key in keys
                if multi[key].value]

    async def get_partial_raw(self, key, *paths):
        values = await self.bucket.retrieve_in(key, *paths)
        return dict(zip(paths, values))

    async def close(self):
        pass
