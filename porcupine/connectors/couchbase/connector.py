import asyncio
import random
import ujson

import couchbase.subdocument as sub_doc
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import NotFoundError, DocumentNotJsonError, \
    SubdocPathNotFoundError, KeyExistsError

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

    def get_query(self, query, ad_hoc=True, **kwargs):
        n1query = N1QLQuery(query, **kwargs)
        n1query.adhoc = ad_hoc
        return self.bucket.n1ql_query(n1query)

    async def exists(self, key):
        try:
            await self.bucket.retrieve_in(key, 'dl')
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

    async def insert_multi(self, insertions):
        try:
            await self.bucket.insert_multi(insertions,
                                           format=couchbase.FMT_AUTO)
        except KeyExistsError as e:
            existing_key = e.key
            inserted = [key
                        for key, result in e.all_results.items()
                        if result.rc == 0]
            # rollback
            if inserted:
                await self.delete_multi(inserted)
            self.raise_exists(existing_key)

    def upsert_multi(self, upsertions):
        return self.bucket.upsert_multi(upsertions, format=couchbase.FMT_AUTO)

    def delete_multi(self, deletions):
        return self.bucket.remove_multi(deletions, quiet=True)

    def mutate_in(self, item_id: str, mutations_dict: dict):
        mutations = []
        for path, mutation in mutations_dict.items():
            mutation_type, value = mutation
            if mutation_type == self.SUB_DOC_UPSERT_MUT:
                mutations.append(sub_doc.upsert(path, value))
            elif mutation_type == self.SUB_DOC_COUNTER:
                mutations.append(sub_doc.counter(path, value))
        return self.bucket.mutate_in(item_id, *mutations)

    async def append_multi(self, appends):
        # make sure keys are initialized
        # tasks = [self.exists(key)
        #          for key in appends]
        # completed, _ = await asyncio.wait(tasks)
        # keys_exist = [c.result() for c in completed]
        # initializations = {key: '' for key, exists in keys_exist
        #                    if not exists}
        # if initializations:
        try:
            await self.bucket.insert_multi({key: '' for key in appends},
                                           format=couchbase.FMT_AUTO)
        except KeyExistsError:
            pass
        await self.bucket.append_multi(appends)

    async def swap_if_not_modified(self, key, xform):
        try:
            result = await self.bucket.get(key)
        except NotFoundError:
            raise exceptions.NotFound('Key {0} is removed'.format(key))
        xform_result = xform(result.value)
        if asyncio.iscoroutine(xform_result):
            xform_result = await xform_result
        new_value, return_value = xform_result
        if new_value is not None:
            try:
                await self.bucket.replace(key, new_value,
                                          cas=result.cas,
                                          format=couchbase.FMT_AUTO)
            except KeyExistsError:
                return False, None
            except NotFoundError:
                raise exceptions.NotFound('Key {0} is removed'.format(key))
        return True, return_value

    # indexes
    def prepare_indexes(self):
        log.info('Preparing indexes')
        self.connect(async=False)
        # get existing indexes
        mgr = self.bucket.bucket_manager()
        existing = [index.name for index in mgr.list_n1ql_indexes()]
        new_indexes = [ind for name, ind in self.indexes.items()
                       if name not in existing]
        # create new indexes
        for index in new_indexes:
            log.info('Creating index {0}'.format(index.name))
            mgr.create_n1ql_index(index.name,
                                  fields=[index.key],
                                  defer=True,
                                  ignore_exists=True)
        # build new indexes
        if new_indexes:
            new_indexes_names = [index.name for index in new_indexes]
            log.info('Building indexes {0}'.format(new_indexes_names))
            mgr.build_n1ql_deferred_indexes()
            mgr.watch_n1ql_indexes(new_indexes_names, timeout=120)
        # drop old indexes
        old_indexes = [ind_name for ind_name in existing
                       if ind_name not in self.indexes]
        for index in old_indexes:
            log.info('Dropping index {0}'.format(index))
            mgr.drop_n1ql_index(index, ignore_missing=True)

    async def close(self):
        pass
