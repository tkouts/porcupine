import asyncio
from inspect import isawaitable

from collections import defaultdict

from porcupine import exceptions, gather
from porcupine.core import utils


class Transaction:
    __slots__ = ('connector', 'options',
                 '_inserted_items', '_items', '_deleted_items',
                 '_insertions', '_upsertions', '_deletions',
                 '_sd', '_appends')

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._inserted_items = {}
        self._items = {}
        self._deleted_items = {}
        self._insertions = {}
        self._upsertions = {}
        self._deletions = {}

        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = defaultdict(list)

    def __contains__(self, key):
        return key in self._items \
               or key in self._inserted_items \
               or key in self._deleted_items \
               or key in self._insertions \
               or key in self._deletions \
               or key in self._upsertions

    def __getitem__(self, key):
        if key in self._deletions or key in self._deleted_items:
            return None
        elif key in self._inserted_items:
            return self._inserted_items[key]
        elif key in self._items:
            return self._items[key]
        elif key in self._insertions:
            return self._insertions[key]
        elif key in self._upsertions:
            return self._upsertions[key]
        raise KeyError

    def insert(self, item):
        if item.id in self._inserted_items:
            self.connector.raise_exists(item.id)
        self._inserted_items[item.id] = item

    def upsert(self, item):
        if item.id not in self._inserted_items:
            self._items[item.id] = item

    def delete(self, item):
        if item.id in self._inserted_items:
            del self._inserted_items[item.id]
        else:
            if item.id in self._items:
                del self._items[item.id]
            self._deleted_items[item.id] = item

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = (mutation_type, value)

    def append(self, key, value):
        if value not in self._appends[key]:
            self._appends[key].append(value)

    def insert_external(self, key, value):
        if key in self._insertions:
            self.connector.raise_exists(key)
        self._insertions[key] = value

    def put_external(self, key, value):
        if key in self._insertions:
            self._insertions[key] = value
            return
        self._upsertions[key] = value

    def delete_external(self, key):
        if key in self._insertions:
            del self._insertions[key]
        else:
            if key is self._upsertions:
                del self._upsertions[key]
            self._deletions[key] = True

    async def prepare(self):
        connector = self.connector
        dumps = connector.persist.dumps
        deletions = []
        insertions = {}
        # call event handlers
        # till insertions, snapshots and deletions are drained
        while True:
            inserted_items = list(self._inserted_items.values())

            insertions.update({i.__storage__.id: dumps(i)
                               for i in inserted_items})

            snapshots = {i.__storage__.id: i.__snapshot__
                         for i in self._items.values()
                         if i.__snapshot__}

            # clear snapshots
            for item in inserted_items:
                item.__reset__()
                # clear new flag
                item.__is_new__ = False
                # add to items so that later can be modified
                self._items[item.id] = item
            for item_id in snapshots:
                self._items[item_id].__reset__()

            removed_items = list(self._deleted_items.values())
            deletions.extend(self._deleted_items.keys())

            # clear inserted items
            self._inserted_items = {}

            # clear deleted items
            self._deleted_items = {}

            # print(snapshots, removed_items)
            if inserted_items or snapshots or removed_items:
                async_handlers = []
                for item in inserted_items:
                    # execute on_create handlers
                    for data_type in item.__schema__.values():
                        try:
                            _ = data_type.on_create(item,
                                                    data_type.get_value(item))
                            if asyncio.iscoroutine(_):
                                async_handlers.append(_)
                        except exceptions.AttributeSetError as e:
                            raise exceptions.InvalidUsage(str(e))

                for item_id, snapshot in snapshots.items():
                    # execute on change handlers
                    item = self._items[item_id]
                    for attr, old_value in snapshot.items():
                        data_type = utils.get_descriptor_by_storage_key(
                            item.__class__, attr)
                        try:
                            _ = data_type.on_change(item,
                                                    data_type.get_value(item),
                                                    old_value)
                            if asyncio.iscoroutine(_):
                                async_handlers.append(_)
                        except exceptions.AttributeSetError as e:
                            raise exceptions.InvalidUsage(str(e))

                for item in removed_items:
                    # execute on delete handlers
                    for dt in list(item.__schema__.values()):
                        _ = dt.on_delete(item, dt.get_value(item))
                        if asyncio.iscoroutine(_):
                            async_handlers.append(_)

                if async_handlers:
                    # execute async handlers
                    await gather(*async_handlers)
            else:
                break
        # update insertions with externals
        insertions.update(self._insertions)
        # external upsertions
        upsertions = self._upsertions

        # update deletions with externals
        deletions.extend(self._deletions.keys())
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
            await connector.insert_multi(insertions)

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

    async def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        self.connector.active_txns -= 1
