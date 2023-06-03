import asyncio
import random
import time
from datetime import timedelta

import orjson
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import (
    ClusterOptions,
    GetOptions,
    InsertOptions,
    UpsertOptions,
    MutateInOptions,
    ReplaceOptions,
)
from couchbase.transcoder import (
    RawJSONTranscoder,
    RawStringTranscoder,
    RawBinaryTranscoder,
)
import couchbase.subdocument as sub_doc
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    DocumentExistsException,
    CASMismatchException,
    HTTPException,
)
from couchbase.management.views import View
from couchbase.management.views import DesignDocumentNamespace
from couchbase.management.search import SearchIndex

from porcupine import exceptions, log
from porcupine.core.context import context_cacheable
from porcupine.core.utils import default_json_encoder
from porcupine.connectors.base.connector import BaseConnector
from porcupine.connectors.couchbase.management.views import \
    DesignDocumentWithOptions
from porcupine.connectors.couchbase.viewindex import Index
from porcupine.connectors.couchbase.ftsindex import FTSIndex
from porcupine.connectors.mutations import Formats, SubDocument


transcoders = {
    Formats.JSON: RawJSONTranscoder(),
    Formats.STRING: RawStringTranscoder(),
    Formats.BINARY: RawBinaryTranscoder(),
}


def json_dumps(obj):
    return orjson.dumps(obj, default=default_json_encoder).decode()


class Couchbase(BaseConnector):
    IndexType = Index
    FTSIndexType = FTSIndex

    def __init__(self, server):
        super().__init__(server)
        self.cluster = None
        self.bucket = None
        self.collection = None

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

    def _get_cluster(self):
        hosts = self.server.config.DB_HOST.split(',')
        random.shuffle(hosts)
        connection_string = f'{self.protocol}://{",".join(hosts)}'
        auth = PasswordAuthenticator(self.user_name, self.password)
        cluster = Cluster(
            connection_string,
            ClusterOptions(auth)
        )
        return cluster

    async def connect(self):
        self.cluster = self._get_cluster()
        self.bucket = self.cluster.bucket(self.bucket_name)
        await self.bucket.on_connect()
        self.collection = self.bucket.default_collection()

    @context_cacheable(size=1024)
    async def key_exists(self, key) -> bool:
        result = await self.collection.exists(key)
        return result.exists

    async def get_raw(self, key, fmt=Formats.JSON, quiet=True):
        try:
            result = await self.collection.get(
                key,
                GetOptions(transcoder=transcoders[fmt]),
            )
        except HTTPException:
            # TODO: fix replica reads
            # getting from replica
            result = await self.collection.rget(key, quiet=quiet)
        except DocumentNotFoundException:
            if quiet:
                return None
            else:
                raise exceptions.NotFound(
                    'The resource {0} does not exist'.format(key))
        except CouchbaseException as e:
            raise exceptions.DBError from e
        if fmt is Formats.JSON and result.value is not None:
            return orjson.loads(result.value)
        return result.value

    async def insert_raw(self, key, value, ttl=None, fmt=Formats.JSON):
        if fmt is Formats.JSON:
            value = json_dumps(value)
        options = InsertOptions(transcoder=transcoders[fmt])
        if ttl is not None:
            if isinstance(ttl, int):
                ttl = timedelta(seconds=ttl - time.time())
            options['expiry'] = ttl
        # print(key, ttl)
        try:
            await self.collection.insert(key, value, options)
        except DocumentExistsException as e:
            self.raise_exists(key, e)
        except CouchbaseException as e:
            raise exceptions.DBError from e

    async def upsert_raw(self, key, value, ttl=None, fmt=Formats.JSON):
        if fmt is Formats.JSON:
            value = json_dumps(value)
        options = UpsertOptions(
            transcoder=transcoders[fmt],
            preserve_expiry=True,
        )
        if ttl is not None:
            if isinstance(ttl, int):
                ttl = timedelta(seconds=ttl - time.time())
            options['expiry'] = ttl
        try:
            await self.collection.upsert(key, value, options)
        except CouchbaseException as e:
            raise exceptions.DBError from e

    async def append_raw(self, key, value, ttl=None, fmt=Formats.STRING):
        binary_collection = self.collection.binary()
        try:
            await binary_collection.append(key, value)
        except DocumentNotFoundException:
            try:
                await self.insert_raw(key, value, ttl, fmt)
            except exceptions.DBAlreadyExists:
                # another coro created the document
                try:
                    await binary_collection.append(key, value)
                except CouchbaseException as e:
                    raise exceptions.DBError from e
            except CouchbaseException as e:
                raise exceptions.DBError from e
        except CouchbaseException as e:
            raise exceptions.DBError from e

    async def delete(self, key):
        try:
            await self.collection.remove(key)
        except DocumentNotFoundException:
            pass
        except CouchbaseException as e:
            raise exceptions.DBError from e

    async def mutate_in(self, item_id: str, mutations_dict: dict):
        mutations = []
        mutate_options = MutateInOptions(
            preserve_expiry=True
        )
        for path, mutation in mutations_dict.items():
            mutation_type, value = mutation
            if mutation_type is SubDocument.UPSERT:
                mutations.append(sub_doc.upsert(path, value))
            elif mutation_type is SubDocument.COUNTER:
                mutations.append(sub_doc.counter(path, value))
            elif mutation_type is SubDocument.INSERT:
                mutations.append(sub_doc.insert(path, value))
            elif mutation_type is SubDocument.REMOVE:
                mutations.append(sub_doc.remove(path))
        try:
            await self.collection.mutate_in(item_id, mutations, mutate_options)
        except CouchbaseException as e:
            raise exceptions.DBError from e

    async def swap_if_not_modified(self, key, xform, fmt, ttl=None):
        transcoder = transcoders[fmt]
        try:
            result = await self.collection.get(
                key,
                GetOptions(transcoder=transcoder),
            )
        except DocumentNotFoundException:
            raise exceptions.NotFound(f'Key {key} is removed')
        xform_result = xform(result.value)
        if asyncio.iscoroutine(xform_result):
            xform_result = await xform_result
        new_value, return_value = xform_result
        if new_value is not None:
            try:
                await self.collection.replace(
                    key, new_value,
                    ReplaceOptions(
                        transcoder=transcoder,
                        cas=result.cas,
                        preserve_expiry=True,
                    )
                )
            # TODO: use CasMismatchException?
            except (DocumentExistsException, CASMismatchException):
                return False, None
            except DocumentNotFoundException:
                raise exceptions.NotFound(f'Key {key} is removed')
        return True, return_value

    def config(self):
        config = self.server.config
        return {
            'views': {
                'updateInterval':
                    int(config.COUCH_VIEWS_UPDATE_INTERVAL),
                'updateMinChanges':
                    int(config.COUCH_VIEWS_UPDATE_MIN_CHANGES),
                'replicaUpdateMinChanges':
                    int(config.COUCH_VIEWS_REPLICA_UPDATE_MIN_CHANGES)
            }
        }

    # indexes
    async def prepare_indexes(self):
        log.info('Preparing indexes...')
        cluster = self.cluster
        bucket = self.bucket
        views_mgr = bucket.view_indexes()

        old_indexes = set()
        new_indexes = set()

        # use production namespace
        namespace = DesignDocumentNamespace.PRODUCTION

        # get current indexes
        for design_doc in await views_mgr.get_all_design_documents(namespace):
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
            await views_mgr.upsert_design_document(design_doc, namespace)

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
        await views_mgr.upsert_design_document(design_doc, namespace)

        # remove unused
        for_removal = old_indexes - new_indexes
        for design in for_removal:
            log.info(f'Dropping view index {design}')
            await views_mgr.drop_design_document(design, namespace)

        search_mgr = cluster.search_indexes()

        old_fts_indexes = set()
        new_fts_indexes = set()

        # get current indexes
        existing_indexes = {}
        try:
            current_fts_indexes = await search_mgr.get_all_indexes()
        except TypeError:
            current_fts_indexes = {}
        for fts_index in current_fts_indexes:
            if fts_index.source_name == self.bucket_name:
                old_fts_indexes.add(fts_index.name)
                existing_indexes[fts_index.name] = fts_index

        # create FTS indexes
        for container_type, index in self.indexes['fts'].items():
            search_index_name = container_type.__name__
            index_params = index.get_params()

            # check if we need to create/update
            existing_index = existing_indexes.get(search_index_name)
            should_rebuild = existing_index is not None and \
                existing_index.params != index_params

            if existing_index is None or should_rebuild:
                if should_rebuild:
                    log.info(f'Rebuilding FTS index {search_index_name}')
                    await search_mgr.drop_index(search_index_name)

                search_index = SearchIndex(
                    name=search_index_name,
                    source_name=self.bucket_name,
                    params=index_params
                )
                await search_mgr.upsert_index(search_index)
            new_fts_indexes.add(search_index_name)

        # remove old indexes
        for_removal = old_fts_indexes - new_fts_indexes
        for search_index in for_removal:
            log.info(f'Dropping FTS index {search_index}')
            await search_mgr.drop_index(search_index)

    async def truncate(self, **options):
        ...

    async def close(self):
        pass
