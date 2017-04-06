import random
import ujson

import couchbase.subdocument as SD
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import NotFoundError, DocumentNotJsonError, \
    SubdocPathNotFoundError, KeyExistsError, HTTPError

from porcupine import exceptions, log
from porcupine.core.abstract.connector import AbstractConnector
from .cursor import Cursor
from .index import Index
import couchbase.experimental
couchbase.experimental.enable()
from acouchbase.bucket import Bucket as aBucket


couchbase.set_json_converters(ujson.dumps, ujson.loads)


class Couchbase(AbstractConnector):
    CursorType = Cursor
    IndexType = Index

    def __init__(self):
        super().__init__()
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

    def connect(self, async=True):
        hosts = self.settings['hosts'][:]
        random.shuffle(hosts)
        connection_string = '{0}://{1}/{2}'.format(self.protocol,
                                                   ','.join(hosts),
                                                   self.bucket_name)
        if async:
            bucket = aBucket
        else:
            bucket = Bucket
        self.bucket = bucket(connection_string,
                             password=self.password)
        if async:
            return self.bucket.connect()

    def get_query(self, query, **kwargs):
        n1query = N1QLQuery(query, **kwargs)
        return self.bucket.n1ql_query(n1query)

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

    # indexes
    def prepare_indexes(self):
        log.info('Preparing indexes')
        self.connect(async=False)
        # get existing indexes
        existing = []
        query = 'SELECT name FROM system:indexes WHERE keyspace_id=$bucket'
        for result in self.get_query(query, bucket=self.bucket_name):
            existing.append(result['name'])
        new_indexes = [ind for name, ind in self.indexes.items()
                       if name not in existing]
        # create secondary indexes
        for index in new_indexes:
            index.create()
        if new_indexes:
            self.build_indexes(*new_indexes)

    def build_indexes(self, *indexes):
        new_indexes_names = ','.join(
            [ind.data_type.name for ind in indexes])
        log.info('Building indexes [{0}]'.format(new_indexes_names))
        build_query = 'BUILD INDEX ON `{0}`({1});'.format(
            self.bucket_name, new_indexes_names)
        try:
            self.get_query(build_query).execute()
        except HTTPError as e:
            message = e.objextra.value['errors'][0]['msg'].lower()
            if 'already' not in message:
                raise

    async def close(self):
        pass
