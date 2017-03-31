import ujson

import couchbase.subdocument as SD
import couchbase.experimental
import random
from couchbase.exceptions import NotFoundError, DocumentNotJsonError, \
    SubdocPathNotFoundError, KeyExistsError

from porcupine import exceptions
from porcupine.core.abstract.connector import AbstractConnector
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
            await self.bucket.retrieve_in(key, '/')
        except NotFoundError:
            return key, False
        except (DocumentNotJsonError, SubdocPathNotFoundError):
            pass
        return key, True

    async def get_raw(self, key, quiet=True):
        try:
            result = await self.bucket.get(key, quiet=quiet)
        except NotFoundError:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(key))
        return result.value

    async def get_multi_raw(self, keys):
        multi = await self.bucket.get_multi(keys, quiet=True)
        return [multi[key].value for key in keys]

    async def get_partial_raw(self, key, *paths):
        values = await self.bucket.retrieve_in(key, *paths)
        return dict(zip(paths, values))

    def insert_multi(self, insertions):
        return self.bucket.insert_multi(insertions, format=couchbase.FMT_AUTO)

    def upsert_multi(self, upsertions):
        return self.bucket.upsert_multi(upsertions, format=couchbase.FMT_AUTO)

    def mutate_in(self, item_id: str, mutations_dict: dict):
        mutations = []
        for path, mutation in mutations_dict.items():
            mutation_type, value = mutation
            if mutation_type == self.SUB_DOC_UPSERT_MUT:
                mutations.append(SD.upsert(path, value))
            elif mutation_type == self.SUB_DOC_COUNTER:
                mutations.append(SD.counter(path, value))
        return self.bucket.mutate_in(item_id, *mutations)

    def append_multi(self, appends):
        return self.bucket.append_multi(appends)

    async def swap_if_not_modified(self, key, xform):
        try:
            result = await self.bucket.get(key)
        except NotFoundError:
            raise exceptions.NotFound
        new_value, return_value = xform(result.value)
        if new_value is not None:
            try:
                await self.bucket.replace(key, new_value,
                                          cas=result.cas,
                                          format=couchbase.FMT_AUTO)
            except KeyExistsError:
                return False, None
            except NotFoundError:
                raise exceptions.NotFound
        return True, return_value

    async def close(self):
        pass
