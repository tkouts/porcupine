import abc

from porcupine import context
from porcupine.core import utils
from porcupine.core.utils.collections import FrozenDict
from porcupine.exceptions import DBAlreadyExists
from porcupine.connectors.base.transaction import Transaction
from porcupine.connectors.base.persist import DefaultPersistence
# from porcupine.connectors.base.join import Join


class BaseConnector(metaclass=abc.ABCMeta):
    active_txns = 0
    TransactionType = Transaction
    IndexType = None
    persist = DefaultPersistence
    supports_ttl = True

    # Sub Document Mutation Codes
    SUB_DOC_UPSERT_MUT = 0
    SUB_DOC_COUNTER = 1
    SUB_DOC_INSERT = 2
    SUB_DOC_REMOVE = 4

    @staticmethod
    def raise_exists(key):
        if '>' in key:
            # unique constraint
            container_id, attr_name, _ = key.split('>')
            raise DBAlreadyExists(
                f'A resource having the same {attr_name} '
                f'in {container_id} already exists')
        else:
            # item
            raise DBAlreadyExists(
                f'A resource having an ID of {key} already exists')

    def __init__(self, server):
        # configuration
        self.server = server
        self.multi_fetch_chunk_size = int(server.config.DB_MULTI_FETCH_SIZE)
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
            indexed_data_types = self.server.config.__indices__
            self.__indexes = FrozenDict({
                k: self.get_index(v)
                for k, v in indexed_data_types.items()
            })
        return self.__indexes

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    async def get(self, object_id, quiet=True):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        item = context.db_cache.get(object_id)
        if item is None:
            item = await self.get_raw(object_id, quiet=quiet)
            if item is not None:
                item = self.persist.loads(item)
            context.db_cache[object_id] = item
        return item

    async def get_multi(self, object_ids):
        loads = self.persist.loads
        if object_ids:
            for chunk in utils.chunks(list(object_ids),
                                      self.multi_fetch_chunk_size):
                fetched = {}
                for item_id in chunk:
                    if context.txn is not None and item_id in context.txn:
                        fetched[item_id] = context.txn[item_id]
                    item = context.db_cache.get(item_id)
                    if item is not None:
                        fetched[item_id] = context.db_cache[item_id]
                db_fetch_keys = [item_id for item_id in chunk
                                 if item_id not in fetched]
                db_fetch = {}
                if db_fetch_keys:
                    db_fetch = await self.get_multi_raw(db_fetch_keys)
                for item_id in chunk:
                    if item_id in fetched:
                        yield item_id, fetched[item_id]
                    else:
                        raw_item = db_fetch[item_id]
                        if raw_item is None:
                            context.db_cache[item_id] = None
                            yield item_id, None
                        else:
                            item = loads(raw_item)
                            context.db_cache[item_id] = item
                            yield item_id, item

    async def get_external(self, ext_id):
        if context.txn is not None and ext_id in context.txn:
            return context.txn[ext_id]
        elif ext_id in context.db_cache:
            return context.db_cache[ext_id]
        ext = await self.get_raw(ext_id)
        context.db_cache[ext_id] = ext
        return ext

    async def exists(self, key):
        if context.txn is not None and key in context.txn:
            return key, context.txn[key] is not None
        elif key in context.db_cache:
            return key, context.db_cache[key] is not None
        key_exists = await self.key_exists(key)
        return key, key_exists

    # item operations
    async def key_exists(self, key):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_raw(self, key, quiet=True):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_multi_raw(self, keys):
        raise NotImplementedError

    def insert_multi(self, insertions, ttl=None) -> list:
        raise NotImplementedError

    def upsert_multi(self, upsertions, ttl=None):
        raise NotImplementedError

    def delete_multi(self, deletions):
        raise NotImplementedError

    def touch_multi(self, touches):
        pass

    def mutate_in(self, item_id, mutations_dict: dict):
        raise NotImplementedError

    def append_multi(self, appends):
        raise NotImplementedError

    async def swap_if_not_modified(self, key, xform, ttl=None):
        raise NotImplementedError

    # atomic operations
    # @abc.abstractmethod
    async def get_atomic(self, object_id, name):
        raise NotImplementedError

    # @abc.abstractmethod
    async def set_atomic(self, object_id, name, value):
        raise NotImplementedError

    # @abc.abstractmethod
    async def delete_atomic(self, object_id, name):
        raise NotImplementedError

    # @abc.abstractmethod
    async def increment_atomic(self, object_id, name, amount, default):
        raise NotImplementedError

    # transaction
    def get_transaction(self, **options):
        return self.TransactionType(self, **options)

    # indexes
    @abc.abstractmethod
    def prepare_indexes(self):
        raise NotImplementedError

    def get_index(self, data_type):
        return self.IndexType(self, data_type)

    # def get_cursor(self, index_name, value=None, c_range=None):
    #     cursor = self.CursorType(self, self.indexes[index_name])
    #     if c_range is None:
    #         cursor.set(value)
    #     else:
    #         cursor.set_range(c_range)
    #     return cursor
    #
    # def get_cursor_list(self, conditions):
    #     cur_list = []
    #     for index, value in conditions:
    #         cursor = self.CursorType(self, self.indexes[index])
    #         if isinstance(value, (list, tuple)):
    #             is_reversed = len(value) == 3 and value[2]
    #             cursor.set_range(value[0], value[1])
    #             if is_reversed:
    #                 cursor.reverse()
    #         else:
    #             cursor.set(value)
    #         cur_list.append(cursor)
    #     return cur_list
    #
    # def query(self, conditions):
    #     cur_list = self.get_cursor_list(conditions)
    #     if len(cur_list) == 1:
    #         return cur_list[0]
    #     else:
    #         c_join = Join(self, cur_list)
    #         return c_join

    # management
    # @abc.abstractmethod
    async def truncate(self, **options):
        raise NotImplementedError

    # @abc.abstractmethod
    def close(self):
        raise NotImplementedError
