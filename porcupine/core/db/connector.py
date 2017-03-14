import abc
from porcupine import context, exceptions
from porcupine.config import settings
from porcupine.utils import system
from .persist import DefaultPersistence
from .join import Join


class AbstractConnector(object, metaclass=abc.ABCMeta):
    settings = settings['db']
    indexes = {}
    active_txns = 0
    root_id = ''
    TransactionType = None
    CursorType = None
    persist = DefaultPersistence

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    async def _get_item_by_path(self, path_tokens):
        child_id = ''
        child = None
        for name in path_tokens[1:]:
            if name:
                child = await self.get_child_by_name(child_id, name)
                if child is None:
                    return None
                else:
                    child_id = child.id
        return child

    async def get(self, object_id):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        if object_id.startswith('/'):
            item = None
            path_tokens = object_id.split('/')
            path_depth = len(path_tokens)
            # /[itemID]?
            if path_depth == 2:
                item = await self.get_raw(path_tokens[1])
            # /folder1/folder2/item
            if item is None:
                return await self._get_item_by_path(path_tokens)
        else:
            item = await self.get_raw(object_id)

        if item is not None:
            item = self.persist.loads(item)
            return item

    async def get_partial(self, object_id, *paths, snapshot=False):

        def snapshot_getter(i, p):
            return i.__snapshot__.get(p, i.__storage__[p])

        def normal_getter(i, p):
            return i.__storage__[p]

        if context.txn is not None and object_id in context.txn:
            item = context.txn[object_id]
            if snapshot:
                getter = snapshot_getter
            else:
                getter = normal_getter
            return {path: getter(item, path) for path in paths}
        return await self.get_partial_raw(object_id, *paths)

    async def get_multi(self, object_ids):
        # TODO: fetch from txn
        loads = self.persist.loads
        if object_ids:
            return [loads(item)
                    for item in await self.get_multi_raw(object_ids)
                    if item is not None]
        return []

    async def get_external(self, ext_id):
        if context.txn is not None and ext_id in context.txn:
            return context.txn[ext_id]
        return await self.get_raw(ext_id)

    async def exists(self, key):
        raise NotImplementedError

    # item operations
    # @abc.abstractmethod
    async def get_raw(self, key):
        raise NotImplementedError

    async def get_partial_raw(self, key, *paths):
        raise NotImplementedError

    # @abc.abstractmethod
    async def get_multi_raw(self, keys):
        raise NotImplementedError

    # @abc.abstractmethod
    # async def insert_raw(self, key, value):
    #     raise NotImplementedError

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

    # indices
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

    # event handling
    def handle_update(self, item, old_item,
                      check_unique=True, execute_event_handlers=True):
        if old_item is not None:
            # check for schema modifications
            item.update_schema()
        if execute_event_handlers and item.event_handlers:
            if old_item is not None:
                # update
                for handler in item.event_handlers:
                    handler.on_update(item, old_item)
            else:
                # create
                for handler in item.event_handlers:
                    handler.on_create(item)

        # validate schema
        for attr_name, attr_def in item.__schema__.items():
            attr_def.validate(item)

            new_attr = getattr(item, attr_name)
            if old_item:
                old_attr = getattr(old_item, attr_name)
                old_pid = old_item.parent_id
            else:
                old_attr = None
                old_pid = item.parent_id

            if hasattr(item, '_owner') \
                    and (new_attr != old_attr or item.parent_id != old_pid):
                # check unique constraints
                if attr_name in self.indexes and self.indexes[attr_name].unique:
                    if check_unique:
                        ind = self.indexes[attr_name]
                        doc_id = ind.exists(item.parent_id, new_attr)
                        if doc_id and doc_id != (item.id or self.root_id):
                            context.txn.abort()
                            raise exceptions.DBAlreadyExists
                    if old_attr:
                        atomic_key = '{}_{}'.format(
                            attr_name,
                            system.hash_series(old_attr).hexdigest()
                        )
                        self.delete_atomic(old_pid, atomic_key)
                    if item.parent_id is not None:
                        atomic_key = '{}_{}'.format(
                            attr_name,
                            system.hash_series(new_attr).hexdigest()
                        )
                        self.set_atomic(item.parent_id, atomic_key, item.id)

            # call data type's handler
            if old_item:
                # it is an update
                attr_def.on_update(item, new_attr, old_attr)
            else:
                # it is a new object
                attr_def.on_create(item, new_attr)

    @staticmethod
    def handle_post_update(item, old_item):
        if item.event_handlers:
            if old_item is not None:
                # update
                for handler in item.event_handlers:
                    handler.on_post_update(item, old_item)
            else:
                # create
                for handler in item.event_handlers:
                    handler.on_post_create(item)

    def handle_delete(self, item, is_permanent, execute_event_handlers=True):
        if execute_event_handlers and item.event_handlers:
            for handler in item.event_handlers:
                handler.on_delete(item, is_permanent)

        for attr_name, attr_def in item.__schema__.items():
            try:
                attr = getattr(item, attr_name)
            except AttributeError:
                continue

            if hasattr(item, '_owner'):
                if attr_name in self.indexes \
                        and self.indexes[attr_name].unique \
                        and not item.is_deleted:
                    atomic_key = '{}_{}'.format(
                        attr_name,
                        system.hash_series(attr).hexdigest()
                    )
                    self.delete_atomic(item.parent_id, atomic_key)

            # call data type's handler
            attr_def.on_delete(item, attr, is_permanent)

    @staticmethod
    def handle_post_delete(item, is_permanent):
        if item.event_handlers:
            for handler in item.event_handlers:
                handler.on_post_delete(item, is_permanent)

    def handle_undelete(self, item):
        for attr_name, attr_def in item.__schema__.items():
            try:
                attr = getattr(item, attr_name)
            except AttributeError:
                continue

            if hasattr(item, '_owner'):
                if attr_name in self.indexes and self.indexes[attr_name].unique:
                    ind = self.indexes[attr_name]
                    doc_id = ind.exists(item.parent_id, attr)
                    if doc_id and doc_id != (item.id or self.root_id):
                        context.txn.abort()
                        raise exceptions.DBAlreadyExists
                    atomic_key = '{}_{}'.format(
                        attr_name,
                        system.hash_series(attr).hexdigest()
                    )
                    self.set_atomic(item.parent_id, atomic_key, item.id)

            # call data type's handler
            attr_def.on_undelete(item, attr)

    # management
    # @abc.abstractmethod
    async def truncate(self, **options):
        raise NotImplementedError

    # @abc.abstractmethod
    def close(self):
        raise NotImplementedError
