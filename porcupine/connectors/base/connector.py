import abc
from collections import defaultdict

from aiostream import stream, pipe

from porcupine import context
from porcupine.core.utils.collections import FrozenDict
from porcupine.exceptions import DBAlreadyExists, DBError
from porcupine.connectors.base.transaction import Transaction
from porcupine.connectors.base import persist
from porcupine.connectors.mutations import Formats


class BaseConnector(metaclass=abc.ABCMeta):
    active_txns = 0
    TransactionType = Transaction
    IndexType = None
    FTSIndexType = None
    persist = persist
    supports_ttl = True

    @staticmethod
    def raise_exists(key, cause=None):
        if '>' in key:
            # unique constraint
            container_id, attr_name, _ = key.split('>')
            raise DBAlreadyExists(
                f'A resource having the same {attr_name} '
                f'in {container_id} already exists'
            ) from cause
        else:
            # item
            raise DBAlreadyExists(
                f'A resource having an ID of {key} already exists'
            ) from cause

    def __init__(self, server):
        # configuration
        self.server = server
        self.read_concurrency = \
            int(server.config.DB_READ_CONCURRENCY) or None
        self.write_concurrency = \
            int(server.config.DB_WRITE_CONCURRENCY) or None
        self.coll_compact_threshold = \
            float(server.config.DB_COLLECTION_COMPACT_THRESHOLD)
        self.coll_split_threshold = \
            int(server.config.DB_COLLECTION_SPLIT_THRESHOLD)
        self.txn_max_retries = int(server.config.DB_TXN_MAX_RETRIES)
        self.cache_size = int(server.config.DB_CACHE_SIZE)
        self.__indexes = None

    @property
    def indexes(self):
        if self.__indexes is None:
            # create index map
            config = self.server.config
            indexes = config.__indices__
            fts_indexes = config.__fts_indices__
            index_map = {
                'views': defaultdict(dict),
                'fts': {}
            }
            # views
            for container_type, indexed_attrs in indexes.items():
                for attr_set in indexed_attrs:
                    index = self.get_index(container_type, attr_set)
                    index_map['views'][container_type][index.name] = index
            # fts
            for container_type, indexed_attrs in fts_indexes.items():
                index_map['fts'][container_type] = self.get_fts_index(
                    container_type, indexed_attrs
                )
            self.__indexes = FrozenDict(index_map)
        return self.__indexes

    @property
    def views(self):
        return self.indexes['views']

    @property
    def fts_indexes(self):
        return self.indexes['fts']

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    async def get(self, object_id, fmt=Formats.JSON, quiet=True):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        item = context.db_cache.get(object_id)
        if item is None:
            item = await self.get_raw(object_id, fmt=fmt, quiet=quiet)
            if item is not None and fmt is Formats.JSON:
                item = self.persist.loads(item)
            context.db_cache[object_id] = item
        return item

    async def get_multi(self, object_ids):
        streamer = stream.iterate(object_ids)
        streamer |= pipe.map(self.get, task_limit=self.read_concurrency)
        streamer |= pipe.zip(stream.iterate(object_ids))
        async with streamer.stream() as items:
            async for item, item_id in items:
                yield item_id, item

    async def batch_update(self, updates: list, ordered=False):
        async def _process_update(update):
            try:
                await update.apply(self)
            except DBError as db_error:
                db_error.mutation = update
                return db_error

        streamer = stream.iterate(updates)
        streamer |= pipe.map(_process_update,
                             ordered=ordered,
                             task_limit=self.write_concurrency)
        return await stream.list(streamer)

    async def exists(self, key):
        if context.txn is not None and key in context.txn:
            return key, context.txn[key] is not None
        elif key in context.db_cache:
            return key, context.db_cache[key] is not None
        key_exists = await self.key_exists(key)
        return key, key_exists

    # item operations
    @abc.abstractmethod
    async def key_exists(self, key):
        raise NotImplementedError

    @abc.abstractmethod
    async def get_raw(self, key, fmt=Formats.JSON, quiet=True):
        raise NotImplementedError

    @abc.abstractmethod
    async def insert_raw(self, key, value, ttl=None, fmt=Formats.JSON):
        raise NotImplementedError

    @abc.abstractmethod
    async def upsert_raw(self, key, value, ttl=None, fmt=Formats.JSON):
        raise NotImplementedError

    @abc.abstractmethod
    async def append_raw(self, key, value, ttl=None, fmt=Formats.STRING):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, key):
        raise NotImplementedError

    @abc.abstractmethod
    def mutate_in(self, item_id, mutations_dict: dict):
        raise NotImplementedError

    @abc.abstractmethod
    async def swap_if_not_modified(self, key, xform, fmt):
        raise NotImplementedError

    # transaction
    def get_transaction(self, **options):
        return self.TransactionType(self, **options)

    # indexes
    @abc.abstractmethod
    def prepare_indexes(self):
        raise NotImplementedError

    def get_index(self, container_type, attrs):
        if isinstance(attrs, str):
            attrs = [attrs]
        return self.IndexType(self, container_type, attrs)

    def get_fts_index(self, container_type, attrs):
        return self.FTSIndexType(self, container_type, attrs)

    @abc.abstractmethod
    async def truncate(self, **options):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
