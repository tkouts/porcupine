import asyncio
from inspect import isawaitable
from collections import defaultdict

from porcupine import exceptions
from porcupine.utils import system


class Transaction:
    __slots__ = ('connector', 'options', '_items',
                 '_insertions', '_upsertions',
                 '_deletions', '_sd', '_appends')

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._items = {}
        self._insertions = {}
        self._upsertions = {}
        self._deletions = {}

        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = defaultdict(str)

    def __contains__(self, key):
        return key in self._items \
               or key in self._insertions \
               or key in self._deletions \
               or key in self._upsertions

    def __getitem__(self, key):
        if key in self._deletions:
            return None
        elif key in self._items:
            return self._items[key]
        elif key in self._insertions:
            return self._insertions[key]
        elif key in self._upsertions:
            return self._upsertions[key]
        raise KeyError

    def insert(self, item):
        if item.id in self._items:
            self.raise_exists(item.id)
        self.upsert(item)

    def upsert(self, item):
        self._items[item.id] = item

    def delete(self, item):
        self._deletions[item.id] = True

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = (mutation_type, value)

    def append(self, key, value):
        if value not in self._appends[key]:
            self._appends[key] += value

    def insert_external(self, key, value):
        if key in self._insertions:
            self.raise_exists(key)
        self._insertions[key] = value

    def put_external(self, key, value):
        self._upsertions[key] = value

    def delete_external(self, key):
        self._deletions[key] = True

    @staticmethod
    def raise_exists(key):
        if '/' in key:
            # unique constraint
            _, attr_name, _ = key.split('/')
            raise exceptions.DBAlreadyExists(
                'A resource having the same {0} already exists'
                .format(attr_name))
        else:
            # item
            raise exceptions.DBAlreadyExists(
                'A resource having an id of {0} already exists'.format(key))

    async def prepare(self):
        # call changed attributes event handlers till snapshots are drained
        while True:
            snapshots = {item.id: item.__snapshot__
                         for item in self._items.values()
                         if item.__snapshot__}
            # clear snapshots
            for item_id in snapshots:
                self._items[item_id].__reset__()
            # print(snapshots)
            if snapshots:
                for item_id, snapshot in snapshots.items():
                    item = self._items[item_id]
                    for attr, old_value in snapshot.items():
                        attr_def = system.get_descriptor_by_storage_key(
                            item.__class__, attr)
                        try:
                            storage = getattr(item, attr_def.storage)
                            on_change = attr_def.on_change(
                                item,
                                getattr(storage, attr),
                                old_value)
                            if asyncio.iscoroutine(on_change):
                                await on_change
                        except exceptions.AttributeSetError as e:
                            raise exceptions.InvalidUsage(str(e))
            else:
                break

        connector = self.connector
        dumps = connector.persist.dumps

        # insertions
        insertions = self._insertions
        insertions.update({k: dumps(i)
                           for k, i in self._items.items()
                           if i.__is_new__})

        # upsertions
        upsertions = self._upsertions

        if self._appends:
            # make sure externals with appends are initialized
            append_keys = list(self._appends.keys())
            tasks = [connector.exists(key) for key in append_keys]
            completed, _ = await asyncio.wait(tasks)
            keys_exist = [c.result() for c in completed]
            insertions.update({key: '' for key, exists in keys_exist
                               if not exists})

        # deletions
        deletions = self._deletions.keys()

        return insertions, upsertions, deletions

    async def commit(self):
        """
        Commits the transaction.

        @return: None
        """
        insertions, upsertions, deletions = await self.prepare()
        connector = self.connector

        tasks = []
        # insertions
        if insertions:
            # first transaction phase - make sure all keys are non-existing
            # otherwise rollback successful insertions and raise
            success, existing_key, inserted = \
                await connector.insert_multi(insertions)
            if not success:
                # rollback
                if inserted:
                    await connector.delete_multi(inserted)
                self.raise_exists(existing_key)

        # upsertions
        if upsertions:
            task = connector.upsert_multi(upsertions)
            if isawaitable(task):
                tasks.append(task)

        # sub document mutations
        for item_id, mutations in self._sd.items():
            task = connector.mutate_in(item_id, mutations)
            if isawaitable(task):
                tasks.append(task)

        # appends
        if self._appends:
            task = connector.append_multi(self._appends)
            if isawaitable(task):
                tasks.append(task)

        # deletions
        if deletions:
            task = connector.delete_multi(deletions)
            if isawaitable(task):
                tasks.append(task)

        if tasks:
            completed, _ = await asyncio.wait(tasks)
            errors = [task.exception() for task in tasks]
            if any(errors):
                # TODO: log errors
                pass

        self.connector.active_txns -= 1

    async def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        self.connector.active_txns -= 1
