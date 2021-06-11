import asyncio
import random
import orjson

import couchbase_core.experimental
from couchbase.cluster import PasswordAuthenticator
import couchbase.subdocument as sub_doc
from couchbase.exceptions import (
    DocumentNotFoundException,
    DocumentNotJsonException,
    PathNotFoundException,
    DocumentExistsException,
    NotStoredException,
    NetworkException,
    TimeoutException
)
from couchbase.management.views import View
from couchbase.management.views import DesignDocumentNamespace
from couchbase.management.search import SearchIndex
from couchbase_core.transcoder import FMT_AUTO, FMT_UTF8

from porcupine import exceptions, log
from porcupine.core.context import context_cacheable
from porcupine.core.utils import default_json_encoder
from porcupine.connectors.base.connector import BaseConnector
from porcupine.connectors.couchbase.management.views import \
    DesignDocumentWithOptions
from porcupine.connectors.couchbase.viewindex import Index
from porcupine.connectors.couchbase.ftsindex import FTSIndex

# SDK patch for fixing async appends
from couchbase_core.client import Client
# noinspection PyProtectedMember
if 'append' not in Client._MEMCACHED_OPERATIONS:
    # noinspection PyProtectedMember
    Client._MEMCACHED_OPERATIONS = Client._MEMCACHED_OPERATIONS + ('append', )


def json_dumps(obj):
    return orjson.dumps(obj, default=default_json_encoder).decode()


couchbase_core.experimental.enable()
couchbase_core.set_json_converters(
    json_dumps,
    orjson.loads
)


class Couchbase(BaseConnector):
    IndexType = Index
    FTSIndexType = FTSIndex

    def __init__(self, server):
        super().__init__(server)
        self.cluster = None
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

    def _get_cluster(self, _async=True):
        if _async:
            from acouchbase.cluster import Cluster
        else:
            from couchbase.cluster import Cluster
        hosts = self.server.config.DB_HOST.split(',')
        random.shuffle(hosts)
        connection_string = f'{self.protocol}://{",".join(hosts)}'
        cluster = Cluster(
            connection_string,
            authenticator=PasswordAuthenticator(self.user_name, self.password)
        )
        return cluster

    async def connect(self):
        self.cluster = self._get_cluster()
        self.bucket = self.cluster.bucket(self.bucket_name)
        await self.bucket.on_connect()

    @context_cacheable(size=1024)
    async def key_exists(self, key) -> bool:
        try:
            await self.bucket.retrieve_in(key, 'dl')
        except DocumentNotFoundException:
            return False
        except (DocumentNotJsonException, PathNotFoundException):
            pass
        return True

    async def get_raw(self, key, quiet=True):
        try:
            result = await self.bucket.get(key, quiet=quiet)
        except NetworkException:
            # getting from replica
            result = await self.bucket.rget(key, quiet=quiet)
        except DocumentNotFoundException:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(key))
        return result.value

    async def get_multi_raw(self, keys):
        try:
            multi = await self.bucket.get_multi(keys, quiet=True)
        except NetworkException:
            # getting from replicas
            multi = await self.bucket.rget_multi(keys, quiet=True)
        return {key: multi[key].value for key in multi}

    async def insert_multi(self, insertions: dict, ttl=None) -> list:
        try:
            await self.bucket.insert_multi(insertions,
                                           ttl=ttl or 0,
                                           format=FMT_AUTO)
        except DocumentExistsException as e:
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
                                        format=FMT_AUTO)

    def delete_multi(self, deletions):
        return self.bucket.remove_multi(deletions, quiet=True)

    async def touch_multi(self, touches):
        try:
            await self.bucket.touch_multi(touches)
        except TimeoutException:
            # print('timeout', e)
            pass

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
        return self.bucket.mutate_in(item_id, mutations)

    async def append_multi(self, appends):
        while appends:
            try:
                await self.bucket.append_multi(appends, format=FMT_UTF8)
            except NotStoredException as e:
                inserts = {}
                for key, result in e.all_results.items():
                    if result.success:
                        del appends[key]
                    else:
                        inserts[key] = ''
                try:
                    await self.bucket.insert_multi(inserts, format=FMT_UTF8)
                except DocumentExistsException:
                    pass
            else:
                break

    async def swap_if_not_modified(self, key, xform, ttl=None):
        try:
            result = await self.bucket.get(key)
        except DocumentNotFoundException:
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
                                          format=FMT_AUTO)
            except DocumentExistsException:
                return False, None
            except DocumentNotFoundException:
                raise exceptions.NotFound(f'Key {key} is removed')
        return True, return_value

    def config(self):
        config = self.server.config
        return {
            'views': {
                'v_update_interval':
                    int(config.COUCH_VIEWS_UPDATE_INTERVAL),
                'v_update_min_changes':
                    int(config.COUCH_VIEWS_UPDATE_MIN_CHANGES),
                'v_replica_update_min_changes':
                    int(config.COUCH_VIEWS_REPLICA_UPDATE_MIN_CHANGES)
            }
        }

    # indexes
    async def prepare_indexes(self):
        log.info('Preparing indexes')
        cluster = self._get_cluster(_async=False)
        bucket = cluster.bucket(self.bucket_name)
        views_mgr = bucket.view_indexes()

        old_indexes = set()
        new_indexes = set()

        # use production namespace
        namespace = DesignDocumentNamespace.PRODUCTION

        # get current indexes
        for design_doc in views_mgr.get_all_design_documents(namespace):
            if design_doc.name != '_system':
                old_indexes.add(design_doc.name)

        # create views
        dd_doc_options = self.config()['views']
        for container_type, indexes in self.views.items():
            dd_name = container_type.__name__
            design_doc = DesignDocumentWithOptions(dd_name, {}, dd_doc_options)
            for index in indexes.values():
                design_doc.add_view(index.name, index.get_view())
            new_indexes.add(dd_name)
            views_mgr.upsert_design_document(design_doc, namespace)

        # add system views
        design_doc = DesignDocumentWithOptions('_system', {}, {})
        design_doc.add_view('collection_docs', View("""
            function(d, m) {
                var id = m.id
                if (m.type == "base64" && !id.startsWith("_")
                    && id.lastIndexOf("/") > -1)
                {
                    emit(m.id, m.type);
                }
            }
        """))
        views_mgr.upsert_design_document(design_doc, namespace)

        # remove unused
        for_removal = old_indexes - new_indexes
        for design in for_removal:
            log.info(f'Dropping view index {design}')
            views_mgr.drop_design_document(design, namespace)

        search_mgr = cluster.search_indexes()

        old_fts_indexes = set()
        new_fts_indexes = set()

        # get current indexes
        existing_indexes = {}
        for fts_index in search_mgr.get_all_indexes():
            if fts_index['sourceName'] == self.bucket_name:
                old_fts_indexes.add(fts_index['name'])
                existing_indexes[fts_index['name']] = fts_index

        # create FTS indexes
        for container_type, index in self.indexes['fts'].items():
            search_index_name = container_type.__name__
            index_params = index.get_params()

            # check if we need to create/update
            existing_index = existing_indexes.get(search_index_name)
            should_rebuild = existing_index is not None and \
                existing_index['params'] != index_params

            if existing_index is None or should_rebuild:
                if should_rebuild:
                    log.info(f'Rebuilding FTS index {search_index_name}')
                    search_mgr.drop_index(search_index_name)

                search_index = SearchIndex(
                    name=search_index_name,
                    source_name=self.bucket_name,
                    params=index_params
                )
                search_mgr.upsert_index(search_index)
            new_fts_indexes.add(search_index_name)

        # remove old indexes
        for_removal = old_fts_indexes - new_fts_indexes
        for search_index in for_removal:
            log.info(f'Dropping FTS index {search_index}')
            search_mgr.drop_index(search_index)

    async def truncate(self, **options):
        ...

    async def close(self):
        pass
