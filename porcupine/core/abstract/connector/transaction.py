import asyncio
from inspect import isawaitable
from collections import defaultdict

from porcupine import exceptions
from porcupine.core import utils


class Transaction:
    __slots__ = ('connector', 'options',
                 '_items',
                 '_ext_insertions', '_ext_upsertions',
                 '_deletions',
                 '_sd', '_appends',
                 '_attr_locks')

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._items = {}
        self._ext_insertions = {}
        self._ext_upsertions = {}

        self._deletions = {}
        self._attr_locks = {}

        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = defaultdict(list)

    def __contains__(self, key):
        if key in self._deletions:
            return True
        elif key in self._items:
            return True
        elif key in self._ext_upsertions:
            return True
        elif key in self._ext_insertions:
            return True
        return False

    def __getitem__(self, key):
        if key in self._deletions:
            return None
        elif key in self._items:
            item = self._items[key]
            if item.__snapshot__:
                # changes not persisted - restore originals
                for key, old_value in item.__snapshot__.items():
                    data_type = utils.get_descriptor_by_storage_key(
                        type(item), key)
                    storage = getattr(item, data_type.storage)
                    setattr(storage, key, old_value)
            return item
        elif key in self._ext_upsertions:
            return self._ext_upsertions[key]
        elif key in self._ext_insertions:
            return self._ext_insertions[key]
        raise KeyError(key)

    def get_key_append(self, key):
        if key in self._appends:
            return ''.join(self._appends[key])
        return ''

    def reset_key_append(self, key):
        if key in self._appends:
            del self._appends[key]

    async def insert(self, item):
        if item.id in self._items:
            self.connector.raise_exists(item.id)

        await item.on_create()
        item.__reset__()
        # execute data types on_create handlers
        for data_type in item.__schema__.values():
            try:
                _ = data_type.on_create(item,
                                        data_type.get_value(item))
                if asyncio.iscoroutine(_):
                    await _
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))

        self._items[item.id] = item

    async def upsert(self, item):
        if item.__snapshot__:
            # print(item.id, item.__snapshot__)
            await item.on_change()
            snapshot = item.__snapshot__
            item.__reset__()
            # execute data types on_change handlers
            for key, old_value in snapshot.items():
                data_type = utils.get_descriptor_by_storage_key(type(item), key)
                try:
                    _ = data_type.on_change(item,
                                            data_type.get_value(item),
                                            old_value)
                    if asyncio.iscoroutine(_):
                        await _
                except exceptions.AttributeSetError as e:
                    raise exceptions.InvalidUsage(str(e))
        self._items[item.id] = item

    async def delete(self, item):
        await item.on_delete()
        # execute data types on_delete handlers
        for dt in list(item.__schema__.values()):
            _ = dt.on_delete(item, dt.get_value(item))
            if asyncio.iscoroutine(_):
                await _
        self._deletions[item.id] = None

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = (mutation_type, value)

    def append(self, key, value):
        if key in self._ext_insertions:
            self._ext_insertions[key] += value
        elif value not in self._appends[key]:
            self._appends[key].append(value)

    def insert_external(self, key, value):
        if key in self._ext_insertions:
            self.connector.raise_exists(key)
        self._ext_insertions[key] = value

    def put_external(self, key, value):
        if key in self._ext_insertions:
            self._ext_insertions[key] = value
            return
        self._ext_upsertions[key] = value

    def delete_external(self, key):
        if key in self._ext_insertions:
            del self._ext_insertions[key]
        elif key in self._ext_upsertions:
            del self._ext_upsertions[key]
        self._deletions[key] = None

    async def lock_attribute(self, item, attr_name):
        lock_key = utils.get_attribute_lock_key(item.id, attr_name)
        if lock_key not in self._attr_locks:
            try:
                await self.connector.insert_multi({lock_key: ''}, ttl=20)
            except exceptions.DBAlreadyExists:
                raise exceptions.DBDeadlockError(
                    'Failed to lock {0}'.format(attr_name))
            # add lock key to deletions in order to be released
            self._attr_locks[lock_key] = True

    def prepare(self):
        dumps = self.connector.persist.dumps

        # insertions
        insertions = {key: dumps(item)
                      for key, item in self._items.items()
                      if item.__is_new__}
        # update insertions with externals
        insertions.update({key: v for key, v in self._ext_insertions.items()
                           if v})

        # upsertions
        upsertions = self._ext_upsertions

        # update deletions with externals
        deletions = list(self._deletions.keys())
        # remove locks
        deletions.extend(self._attr_locks.keys())

        return insertions, upsertions, deletions

    async def commit(self) -> None:
        """
        Commits the transaction.

        @return: None
        """
        insertions, upsertions, deletions = self.prepare()
        connector = self.connector

        tasks = []
        # insertions
        if insertions:
            # first transaction phase - make sure all keys are non-existing
            await connector.insert_multi(insertions)

        # upsertions
        if upsertions:
            task = connector.upsert_multi(upsertions)
            if isawaitable(task):
                tasks.append(task)

        # sub document mutations
        # print('SD', self._sd)
        if self._sd:
            for item_id, mutations in self._sd.items():
                task = connector.mutate_in(item_id, mutations)
                if isawaitable(task):
                    tasks.append(task)

        # appends
        # print('Appends', self._appends)
        if self._appends:
            appends = {k: ''.join(v) for k, v in self._appends.items()}
            task = connector.append_multi(appends)
            if isawaitable(task):
                tasks.append(task)

        # deletions
        if deletions:
            task = connector.delete_multi(deletions)
            if isawaitable(task):
                tasks.append(task)

        if tasks:
            completed, _ = await asyncio.wait(tasks)
            errors = [task.exception() for task in completed]
            if any(errors):
                # TODO: log errors
                # print(errors)
                pass

        self.connector.active_txns -= 1

    async def abort(self) -> None:
        """
        Aborts the transaction.

        @return: None
        """
        # release attribute locks
        if self._attr_locks:
            await self.connector.delete_multi(self._attr_locks.keys())
        self.connector.active_txns -= 1
