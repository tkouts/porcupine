import abc

from porcupine import context
from porcupine.config import settings
from porcupine.core.abstract.connector.join import Join
from porcupine.core.abstract.connector.persist import DefaultPersistence
from porcupine.core import utils
from porcupine.exceptions import DBAlreadyExists
from .transaction import Transaction


class AbstractConnector(metaclass=abc.ABCMeta):
    # configuration
    settings = settings['db']
    multi_fetch_chunk_size = settings['multi_fetch_chunk_size']
    coll_compact_threshold = settings['collection_compact_threshold']
    coll_split_threshold = settings['collection_split_threshold']
    txn_max_retries = settings['txn_max_retries']

    indexes = {}
    active_txns = 0
    TransactionType = Transaction
    CursorType = None
    IndexType = None
    persist = DefaultPersistence

    # Sub Document Mutation Codes
    SUB_DOC_UPSERT_MUT = 0
    SUB_DOC_COUNTER = 1

    @staticmethod
    def raise_exists(key):
        if '>' in key:
            # unique constraint
            container_id, attr_name, _ = key.split('>')
            raise DBAlreadyExists(
                'A resource having the same {0} in {1} already exists'
                .format(attr_name, container_id))
        else:
            # item
            raise DBAlreadyExists(
                'A resource having an ID of {0} already exists'.format(key))

    def __init__(self):
        # create index map
        indexed_data_types = self.settings['__indices__']
        self.indexes = {
            k: self.get_index(v)
            for k, v in indexed_data_types.items()
        }

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    async def get(self, object_id, quiet=True):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        item = await self.get_raw(object_id, quiet=quiet)
        if item is not None:
            item = self.persist.loads(item)
            return item

    async def get_partial(self, object_id, *paths, snapshot=False):

        def snapshot_getter(i, p):
            return i.get_snapshot_of(p)

        def normal_getter(i, p):
            return getattr(i.__storage__, p)

        if context.txn is not None and object_id in context.txn:
            item = context.txn[object_id]
            if snapshot:
                getter = snapshot_getter
            else:
                getter = normal_getter
            return {path: getter(item, path) for path in paths}
        return await self.get_partial_raw(object_id, *paths)

    async def get_multi(self, object_ids):
        loads = self.persist.loads
        if context.txn is not None:
            txn = context.txn
            in_txn = ([context.txn[object_id]
                       for object_id in object_ids
                       if object_id in txn])
            for item in in_txn:
                yield item
            object_ids = [object_id for object_id in object_ids
                          if object_id not in txn]
        if object_ids:
            for chunk in utils.chunks(object_ids, self.multi_fetch_chunk_size):
                batch = await self.get_multi_raw(chunk)
                for raw_item in batch:
                    if raw_item is None:
                        yield None
                    else:
                        yield loads(raw_item)

    async def get_external(self, ext_id):
        if context.txn is not None and ext_id in context.txn:
            return context.txn[ext_id]
        return await self.get_raw(ext_id)

    async def exists(self, key):
        if context.txn is not None and key in context.txn:
            return key, True
        key_exists = await self.key_exists(key)
        return key, key_exists

    # item operations
    async def key_exists(self, key):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_raw(self, key, quiet=True):
        raise NotImplementedError

    async def get_partial_raw(self, key, *paths):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_multi_raw(self, keys):
        raise NotImplementedError

    def insert_multi(self, insertions):
        raise NotImplementedError

    def upsert_multi(self, upsertions):
        raise NotImplementedError

    def delete_multi(self, deletions):
        raise NotImplementedError

    def mutate_in(self, item_id, mutations_dict: dict):
        raise NotImplementedError

    def append_multi(self, appends):
        raise NotImplementedError

    async def swap_if_not_modified(self, key, xform):
        raise NotImplementedError

    # @abc.abstractmethod
    # async def delete_raw(self, key, value):
    #     raise NotImplementedError

    # containers
    # @abc.abstractmethod
    async def get_children(self, container_id, deep=False):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_child_id_by_name(self, container_id, name):
        raise NotImplementedError

    async def get_child_by_name(self, container_id, name):
        child_id = await self.get_child_id_by_name(container_id, name)
        if child_id:
            return await self.get(child_id)

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

    # @abc.abstractmethod
    # async def put_external(self, ext_id, stream):
    #     raise NotImplementedError

    # @abc.abstractmethod
    async def delete_external(self, ext_id):
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

    def get_cursor(self, index_name, value=None, c_range=None):
        cursor = self.CursorType(self, self.indexes[index_name])
        if c_range is None:
            cursor.set(value)
        else:
            cursor.set_range(c_range)
        return cursor

    def get_cursor_list(self, conditions):
        cur_list = []
        for index, value in conditions:
            cursor = self.CursorType(self, self.indexes[index])
            if isinstance(value, (list, tuple)):
                is_reversed = len(value) == 3 and value[2]
                cursor.set_range(value[0], value[1])
                if is_reversed:
                    cursor.reverse()
            else:
                cursor.set(value)
            cur_list.append(cursor)
        return cur_list

    def query(self, conditions):
        cur_list = self.get_cursor_list(conditions)
        if len(cur_list) == 1:
            return cur_list[0]
        else:
            c_join = Join(self, cur_list)
            return c_join

    # management
    # @abc.abstractmethod
    async def truncate(self, **options):
        raise NotImplementedError

    # @abc.abstractmethod
    def close(self):
        raise NotImplementedError
