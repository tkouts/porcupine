import asyncio
from inspect import isawaitable
from collections import defaultdict

from porcupine import exceptions, log, server
from porcupine.core import utils
from porcupine.core.context import system_override, with_context, context
from porcupine.core.aiolocals.local import wrap_gather as gather


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
            return self._items[key]
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

    def reset_mutations(self, item, key):
        if item.id in self._sd:
            mutations = self._sd[item.id]
            for path in mutations:
                if path.startswith(key):
                    del mutations[path]

    async def insert(self, item):
        if item.id in self._items:
            self.connector.raise_exists(item.id)

        await item.on_create()

        # execute data types on_create handlers
        for data_type in item.__schema__.values():
            try:
                _ = data_type.on_create(item, data_type.get_value(item))
                if isawaitable(_):
                    await _
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))

        item.__reset__()
        self._items[item.id] = item

    async def upsert(self, item):
        if item.__snapshot__:
            locks = []

            await item.on_change()

            # execute data types on_change handlers
            for key, new_value in item.__snapshot__.items():
                data_type = utils.get_descriptor_by_storage_key(type(item), key)
                if data_type.should_lock:
                    locks.append(data_type)
                try:
                    _ = data_type.on_change(
                        item,
                        new_value,
                        data_type.get_value(item, snapshot=False))
                    if isawaitable(_):
                        await _
                except exceptions.AttributeSetError as e:
                    raise exceptions.InvalidUsage(str(e))

            if not item.__is_new__ and locks and \
                    item.__storage__.pid is not None:
                # try to lock attributes
                # print('LOCKING', locks)
                await self.lock_attributes(item, *[u.name for u in locks])

        item.__reset__()
        self._items[item.id] = item

    async def delete(self, item):
        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await gather(*[self.delete(child) for child in children])

        await item.on_delete()

        data_types = item.__schema__.values()
        # execute data types on_delete handlers
        for dt in data_types:
            _ = dt.on_delete(item, dt.get_value(item))
            if isawaitable(_):
                await _

        self._deletions[item.id] = item

    async def recycle(self, item):
        data_types = item.__schema__.values()

        # execute data types on_recycle handlers
        for dt in data_types:
            _ = dt.on_recycle(item, dt.get_value(item))
            if isawaitable(_):
                await _

        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await gather(*[self.recycle(child) for child in children])

    async def restore(self, item):
        data_types = item.__schema__.values()

        # execute data types on_restore handlers
        for dt in data_types:
            _ = dt.on_restore(item, dt.get_value(item))
            if isawaitable(_):
                await _

        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await gather(*[self.restore(child) for child in children])

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = mutation_type, value

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

    async def lock_attributes(self, item, *attributes):
        lock_keys = [utils.get_attribute_lock_key(item.id, attr_name)
                     for attr_name in attributes]
        # filter out the ones already locked
        multi_insert = {key: '' for key in lock_keys
                        if key not in self._attr_locks}
        if multi_insert:
            try:
                await self.connector.insert_multi(multi_insert, ttl=20)
            except exceptions.DBAlreadyExists:
                raise exceptions.DBDeadlockError('Failed to lock attributes')
            # add lock key to deletions in order to be released
            self._attr_locks.update(multi_insert)

    def prepare(self):
        dumps = self.connector.persist.dumps

        if self.connector.server.debug:
            # check for any non persisted modifications
            modified = [i.friendly_name for i in self._items.values()
                        if i.__snapshot__]
            if modified:
                log.debug('Detected uncommitted changes to {0}'
                          .format(' '.join(modified)))

        inserted_items = []
        modified_items = []
        insertions = {}

        for item_id, item in self._items.items():
            if item.__is_new__:
                inserted_items.append(item)
                insertions[item_id] = dumps(item)
            else:
                modified_items.append(item)

        # update insertions with externals
        insertions.update({key: v for key, v in self._ext_insertions.items()
                           if v})

        # upsertions
        upsertions = self._ext_upsertions

        # deletions
        deleted_items = [item for item in self._deletions.values()
                         if hasattr(item, 'content_class')]

        # update deletions with externals
        deletions = list(self._deletions.keys())
        # remove locks
        deletions.extend(self._attr_locks.keys())

        return (insertions, upsertions, deletions), \
               (inserted_items, modified_items, deleted_items)

    async def commit(self) -> None:
        """
        Commits the transaction.

        @return: None
        """
        # prepare
        db_ops, affected_items = self.prepare()
        insertions, upsertions, deletions = db_ops

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

        # execute post txn event handlers
        actor = context.user
        inserted_items, modified_items, deleted_items = affected_items

        if inserted_items:
            asyncio.ensure_future(
                self._exec_post_handler('on_post_create',
                                        inserted_items, actor))

        if modified_items:
            asyncio.ensure_future(
                self._exec_post_handler('on_post_change',
                                        modified_items, actor))

        if deleted_items:
            asyncio.ensure_future(
                self._exec_post_handler('on_post_delete',
                                        deleted_items, actor))

    @with_context(server.system_user)
    async def _exec_post_handler(self, handler: str, items: list, actor):
        tasks = [getattr(item, handler)(actor) for item in items]
        results = await gather(*tasks, return_exceptions=True)
        errors = [result if isinstance(result, Exception) else None
                  for result in results]
        if any(errors):
            message = 'Uncaught exception in post {0} handler of type {1}\n{2}'
            for i, error in enumerate(errors):
                if error is not None:
                    log.error(message.format(
                        handler.split('_')[-1],
                        items[i].content_class,
                        error
                    ))

    async def abort(self) -> None:
        """
        Aborts the transaction.

        @return: None
        """
        # release attribute locks
        if self._attr_locks:
            await self.connector.delete_multi(self._attr_locks.keys())
        self.connector.active_txns -= 1
