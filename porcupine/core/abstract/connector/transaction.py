import asyncio
from inspect import isawaitable
from collections import defaultdict

from porcupine import exceptions, log, server
from porcupine.core import utils
from porcupine.core.utils import date
from porcupine.core.context import system_override, with_context, context


class Transaction:
    __slots__ = ('connector', 'options',
                 '_items',
                 '_ext_insertions', '_ext_upsertions',
                 '_deletions',
                 '_sd', '_appends',
                 '_attr_locks', '_touches')

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._items = {}
        self._ext_insertions = {}
        self._ext_upsertions = {}

        self._deletions = {}
        self._attr_locks = {}

        self._touches = {}

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
            return self._items[key][1]
        elif key in self._ext_upsertions:
            return self._ext_upsertions[key][1]
        elif key in self._ext_insertions:
            return self._ext_insertions[key][1]
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

        ttl = await item.ttl
        self._items[item.id] = ttl, item

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

            ttl = await item.ttl
            if ttl:
                self._touches[item.id] = ttl

        item.__reset__()
        self._items[item.id] = None, item

    async def touch(self, item):
        # touch has to be fast / no event handlers
        if 'md' not in item.__snapshot__:
            now = date.utcnow().isoformat()
            item.__snapshot__['md'] = now
            if not item.__is_new__:
                self.mutate(item, 'md', self.connector.SUB_DOC_UPSERT_MUT, now)
                ttl = await item.ttl
                if ttl:
                    self._touches[item.id] = ttl

    async def delete(self, item):
        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await asyncio.gather(*[self.delete(c) for c in children])

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
                await asyncio.gather(
                    *[self.recycle(child) for child in children])

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
                await asyncio.gather(
                    *[self.restore(child) for child in children])

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = mutation_type, value

    def append(self, key, value, ttl=None):
        if key in self._ext_insertions:
            self._ext_insertions[key][1] += value
        elif value not in self._appends[key]:
            self._appends[key].append(value)
            if ttl:
                self._touches[key] = ttl

    def insert_external(self, key, value, ttl=None):
        if key in self._ext_insertions:
            self.connector.raise_exists(key)
        self._ext_insertions[key] = [ttl, value]

    def put_external(self, key, value, ttl=None):
        if key in self._ext_insertions:
            self._ext_insertions[key] = [ttl, value]
            return
        self._ext_upsertions[key] = [ttl, value]

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
            desc_locator = utils.locate_descriptor_by_storage_key
            for v in self._items.values():
                _, i = v
                if i.__snapshot__:
                    for storage_key in i.__snapshot__:
                        desc = desc_locator(type(i), storage_key)
                        if desc.get_value(i) != desc.get_value(i, False):
                            log.warn('Detected uncommitted '
                                     f'changes to {i.friendly_name}')

        inserted_items = []
        modified_items = []

        # insertions
        insertions = defaultdict(dict)
        for item_id, v in self._items.items():
            ttl, item = v
            if item.__is_new__:
                inserted_items.append(item)
                insertions[ttl][item_id] = dumps(item)
            else:
                modified_items.append(item)

        # update insertions with externals
        for key, v in self._ext_insertions.items():
            ttl, value = v
            if value:
                insertions[ttl][key] = value

        # upsertions
        upsertions = defaultdict(dict)
        for key, v in self._ext_upsertions.items():
            ttl, value = v
            if value:
                upsertions[ttl][key] = value

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

        # insertions
        if insertions:
            # first transaction phase - make sure all keys are non-existing
            insert_tasks = []
            for ttl in insertions:
                task = connector.insert_multi(insertions[ttl], ttl=ttl)
                if isawaitable(task):
                    insert_tasks.append(task)
            if insert_tasks:
                if len(insert_tasks) == 1:
                    await insert_tasks[0]
                else:
                    results = await asyncio.gather(*insert_tasks,
                                                   return_exceptions=True)
                    errors = [r for r in results if isinstance(r, Exception)]
                    if any(errors):
                        inserted = [key for keys in results
                                    if isinstance(keys, list)
                                    for key in keys]
                        if inserted:
                            await connector.delete_multi(inserted)
                        raise errors[0]

        tasks = []
        # upsertions
        if upsertions:
            for ttl in upsertions:
                task = connector.upsert_multi(upsertions[ttl], ttl=ttl)
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

        if self._touches:
            await connector.touch_multi(self._touches)

        self.connector.active_txns -= 1

        # execute post txn event handlers
        actor = context.user
        inserted_items, modified_items, deleted_items = affected_items

        if inserted_items:
            asyncio.create_task(self._exec_post_handler('on_post_create',
                                inserted_items, actor))

        if modified_items:
            asyncio.create_task(self._exec_post_handler('on_post_change',
                                modified_items, actor))

        if deleted_items:
            asyncio.create_task(self._exec_post_handler('on_post_delete',
                                deleted_items, actor))

    @with_context(server.system_user)
    async def _exec_post_handler(self, handler: str, items: list, actor):
        tasks = [getattr(item, handler)(actor) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
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
