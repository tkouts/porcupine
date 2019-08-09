import asyncio
import random
import ujson

import couchbase.subdocument as sub_doc
import couchbase.experimental
from couchbase.exceptions import NotFoundError, DocumentNotJsonError, \
    SubdocPathNotFoundError, KeyExistsError, NotStoredError, \
    CouchbaseNetworkError
from porcupine import exceptions, log
from porcupine.core.context import context_cacheable
from porcupine.connectors.base.connector import BaseConnector

from .index import Index

couchbase.experimental.enable()
couchbase.set_json_converters(ujson.dumps, ujson.loads)


class Couchbase(BaseConnector):
    IndexType = Index

    def __init__(self, server):
        super().__init__(server)
        self.bucket = None

    @property
    def protocol(self):
        return self.server.config.get('DB_PROTOCOL', 'couchbase')

    @property
    def bucket_name(self):
        return self.server.config.DB_NAME

    @property
    def user_name(self):
        return self.server.config.DB_USER

    @property
    def password(self):
        return self.server.config.DB_PASSWORD

    def _get_bucket(self, _async=True):
        hosts = self.server.config.DB_HOST.split(',')
        random.shuffle(hosts)
        connection_string = '{0}://{1}/{2}'.format(self.protocol,
                                                   ','.join(hosts),
                                                   self.bucket_name)
        if _async:
            from acouchbase.bucket import Bucket
        else:
            from couchbase.bucket import Bucket
        return Bucket(connection_string, password=self.password)

    def connect(self):
        self.bucket = self._get_bucket()
        return self.bucket.connect()

    @context_cacheable(size=1000)
    async def key_exists(self, key) -> bool:
        try:
            await self.bucket.retrieve_in(key, 'dl')
        except NotFoundError:
            return False
        except (DocumentNotJsonError, SubdocPathNotFoundError):
            pass
        return True

    async def get_raw(self, key, quiet=True):
        try:
            result = await self.bucket.get(key, quiet=quiet)
        except CouchbaseNetworkError:
            # getting replica
            result = await self.bucket.get(key, replica=True, quiet=quiet)
        except NotFoundError:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(key))
        return result.value

    async def get_multi_raw(self, keys):
        try:
            multi = await self.bucket.get_multi(keys, quiet=True)
        except CouchbaseNetworkError:
            # getting replicas
            docs = await asyncio.gather(*[self.get_raw(key, quiet=True)
                                          for key in keys])
            return {key: doc for key, doc in zip(keys, docs)}
        return {key: multi[key].value for key in multi}

    async def insert_multi(self, insertions: dict, ttl=None) -> list:
        try:
            await self.bucket.insert_multi(insertions,
                                           ttl=ttl or 0,
                                           format=couchbase.FMT_AUTO)
        except KeyExistsError as e:
            existing_key = e.key
            inserted = [key for key, result in e.all_results.items()
                        if result.success]
            # rollback
            if inserted:
                await self.delete_multi(inserted)
            self.raise_exists(existing_key)
        return list(insertions.keys())

    def upsert_multi(self, upsertions, ttl=None):
        return self.bucket.upsert_multi(upsertions,
                                        ttl=ttl or 0,
                                        format=couchbase.FMT_AUTO)

    def delete_multi(self, deletions):
        return self.bucket.remove_multi(deletions, quiet=True)

    def touch_multi(self, touches):
        return self.bucket.touch_multi(touches)

    def mutate_in(self, item_id: str, mutations_dict: dict):
        mutations = []
        for path, mutation in mutations_dict.items():
            mutation_type, value = mutation
            if mutation_type == self.SUB_DOC_UPSERT_MUT:
                mutations.append(sub_doc.upsert(path, value))
            elif mutation_type == self.SUB_DOC_COUNTER:
                mutations.append(sub_doc.counter(path, value))
            elif mutation_type == self.SUB_DOC_INSERT:
                mutations.append(sub_doc.insert(path, value))
            elif mutation_type == self.SUB_DOC_REMOVE:
                mutations.append(sub_doc.remove(path))
        return self.bucket.mutate_in(item_id, *mutations)

    async def append_multi(self, appends):
        while appends:
            try:
                await self.bucket.append_multi(appends)
            except NotStoredError as e:
                inserts = {}
                for key, result in e.all_results.items():
                    if result.success:
                        del appends[key]
                    else:
                        inserts[key] = ''
                try:
                    await self.bucket.insert_multi(inserts,
                                                   format=couchbase.FMT_AUTO)
                except KeyExistsError:
                    pass
            else:
                break

    async def swap_if_not_modified(self, key, xform, ttl=None):
        try:
            result = await self.bucket.get(key)
        except NotFoundError:
            raise exceptions.NotFound(f'Key {key} is removed')
        xform_result = xform(result.value)
        if asyncio.iscoroutine(xform_result):
            xform_result = await xform_result
        new_value, return_value = xform_result
        if new_value is not None:
            try:
                await self.bucket.replace(key, new_value,
                                          cas=result.cas,
                                          ttl=ttl or 0,
                                          format=couchbase.FMT_AUTO)
            except KeyExistsError:
                return False, None
            except NotFoundError:
                raise exceptions.NotFound(f'Key {key} is removed')
        return True, return_value

    # indexes
    async def prepare_indexes(self):
        # return
        log.info('Preparing indexes')
        config = self.server.config
        bucket = self._get_bucket(_async=False)
        design_doc = {
            'views': {},
            'options': {
                'updateInterval': config.COUCH_VIEWS_UPDATE_INTERVAL,
                'updateMinChanges': config.COUCH_VIEWS_UPDATE_MIN_CHANGES,
                'replicaUpdateMinChanges':
                    config.COUCH_VIEWS_REPLICA_UPDATE_MIN_CHANGES
            }
        }
        mgr = bucket.bucket_manager()
        for index in self.indexes.values():
            design_doc['views'][index.name] = index.get_view()
        mgr.design_create('indexes', design_doc, use_devmode=False)

        # # get existing indexes
        # existing = [index.name for index in mgr.list_n1ql_indexes()]
        # new_indexes = [ind for name, ind in self.indexes.items()
        #                if name not in existing]
        # # create new indexes
        # for index in new_indexes:
        #     log.info('Creating index {0}'.format(index.name))
        #     mgr.create_n1ql_index(index.name,
        #                           fields=[index.key],
        #                           defer=True,
        #                           ignore_exists=True)
        # # build new indexes
        # if new_indexes:
        #     new_indexes_names = [index.name for index in new_indexes]
        #     log.info('Building indexes {0}'.format(new_indexes_names))
        #     mgr.build_n1ql_deferred_indexes()
        #     mgr.watch_n1ql_indexes(new_indexes_names, timeout=120)
        # # drop old indexes
        # old_indexes = [ind_name for ind_name in existing
        #                if ind_name not in self.indexes]
        # for index in old_indexes:
        #     log.info('Dropping index {0}'.format(index))
        #     mgr.drop_n1ql_index(index, ignore_missing=True)

    async def close(self):
        pass
