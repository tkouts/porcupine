import asyncio
import random
from typing import Dict, Optional
from collections import defaultdict
from datetime import timedelta
import libsql_client

from porcupine.hinting import TYPING
from porcupine import exceptions, log, server
from porcupine.connectors.mutations import (
    Formats,
    SubDocument,
    Insertion,
    Upsertion,
    Deletion,
    SubDocumentMutation,
    Append,
)
from porcupine.core import utils
from porcupine.core.utils import date, default_json_encoder
from porcupine.core.context import ctx_txn
from porcupine.core.services import get_service
from porcupine.core.context import system_override, context_user, context


class ExternalDoc:
    __slots__ = 'item_id', 'key', 'value'

    def __init__(self, item_id, key, value):
        self.item_id = item_id
        self.key = key
        self.value = value


class Transaction:
    __slots__ = ('connector', 'options',
                 '_items',
                 '_ext_insertions', '_ext_upsertions',
                 '_deletions',
                 '_sd', '_appends',
                 '_attr_locks', '_committed')

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._items: Dict[str, TYPING.ANY_ITEM_CO] = {}
        self._ext_insertions: Dict[str, ExternalDoc] = {}
        self._ext_upsertions: Dict[str, ExternalDoc] = {}

        self._deletions: Dict[str, Optional[TYPING.ANY_ITEM_CO]] = {}
        self._attr_locks = {}

        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = {}

        self._committed = False

    @property
    def committed(self):
        return self._committed

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
            return self._ext_upsertions[key].value
        elif key in self._ext_insertions:
            return self._ext_insertions[key].value
        raise KeyError(key)

    def get_key_append(self, item_id, key):
        item_appends = self._appends.get(item_id)
        if item_appends is not None and key in item_appends:
            return ''.join(item_appends[key])
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

    async def insert(self, item: TYPING.ANY_ITEM_CO):
        if item.id in self._items:
            self.connector.raise_exists(item.id)

        self._items[item.id] = item

        await item.on_create()

        # execute data types on_create handlers
        try:
            await asyncio.gather(*[
                data_type.on_create(item, data_type.get_value(item))
                for data_type in item.__schema__.values()
            ])
            # for data_type in item.__schema__.values():
            #     print(item.content_class, data_type.name)
            #     await data_type.on_create(item, data_type.get_value(item))
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))

        item.__reset__()

    async def upsert(self, item: TYPING.ANY_ITEM_CO):
        if item.__snapshot__:
            await item.on_change()

            # execute data types on_change handlers
            locks = []
            on_change_handlers = []
            for key, new_value in item.__snapshot__.items():
                data_type = utils.get_descriptor_by_storage_key(type(item), key)
                if data_type.should_lock:
                    locks.append(data_type)
                on_change_handlers.append(
                    data_type.on_change(
                        item,
                        new_value,
                        data_type.get_value(item, snapshot=False)
                    )
                )

            try:
                await asyncio.gather(*on_change_handlers)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))

            if (
                not item.__is_new__
                and locks
                and item.__storage__.pid is not None
            ):
                # try to lock attributes
                # print('LOCKING', locks)
                await self.lock_attributes(item, *[u.name for u in locks])

        item.__reset__()
        self._items[item.id] = item

    async def touch(self, item):
        # touch has to be fast / no event handlers
        now = date.utcnow()  # .isoformat()
        if item.__is_new__:
            item.__storage__.md = now
        elif 'md' not in item.__snapshot__:
            item.__snapshot__['md'] = now
            self.mutate(item, 'md', SubDocument.UPSERT, now)
            # add to items map
            self._items[item.id] = item

    async def delete(self, item):
        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await asyncio.gather(*[self.delete(c) for c in children])

        await item.on_delete()

        # execute data types on_delete handlers
        await asyncio.gather(*[
            dt.on_delete(item, dt.get_value(item))
            for dt in item.__schema__.values()
        ])

        self._deletions[item.id] = item

    async def recycle(self, item):
        # execute data types on_recycle handlers
        await asyncio.gather(*[
            dt.on_recycle(item, dt.get_value(item))
            for dt in item.__schema__.values()
        ])

        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await asyncio.gather(
                    *[self.recycle(child) for child in children]
                )

    async def restore(self, item):
        # execute data types on_restore handlers
        await asyncio.gather(*[
            dt.on_restore(item, dt.get_value(item))
            for dt in item.__schema__.values()
        ])

        # add to items so that ttl can be calculated
        self._items[item.id] = item

        if item.is_collection:
            with system_override():
                children = await item.get_children()
                await asyncio.gather(
                    *[self.restore(child) for child in children]
                )

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = (
            mutation_type,
            default_json_encoder(value) or value,
        )

    def append(self, item_id, key, value):
        if key in self._ext_insertions:
            self._ext_insertions[key].value += value
        else:
            item_appends = self._appends.setdefault(item_id, defaultdict(list))
            if value not in item_appends[key]:
                item_appends[key].append(value)

    def insert_external(self, item_id, key, value):
        if key in self._ext_insertions:
            self.connector.raise_exists(key)
        self._ext_insertions[key] = ExternalDoc(item_id, key, value)

    def put_external(self, item_id, key, value):
        doc = ExternalDoc(item_id, key, value)
        if key in self._ext_insertions:
            self._ext_insertions[key] = doc
            return
        self._ext_upsertions[key] = doc

    def delete_external(self, key):
        if key in self._ext_insertions:
            del self._ext_insertions[key]
        elif key in self._ext_upsertions:
            del self._ext_upsertions[key]
        self._deletions[key] = None

    async def insert_multi(self, insertions):
        connector = self.connector
        errors = await connector.batch_update(insertions, ordered=True)
        if any(errors):
            # some insertion(s) failed - roll back
            deletions = [
                Deletion(i.key)
                for (r, i) in zip(errors, insertions)
                if r is None
            ]
            await connector.batch_update(deletions)
            raise next(r for r in errors if r is not None)

    async def lock_attributes(self, item, *attributes):
        lock_keys = [utils.get_attribute_lock_key(item.id, attr_name)
                     for attr_name in attributes]
        # filter out the ones already locked
        multi_insert = {key: None for key in lock_keys
                        if key not in self._attr_locks}
        if multi_insert:
            inserts = [
                Insertion(key, b'', timedelta(seconds=20), Formats.BINARY)
                for key in multi_insert
            ]
            try:
                await self.insert_multi(inserts)
            except exceptions.DBAlreadyExists:
                raise exceptions.DBDeadlockError('Failed to lock attributes')
            # add lock key to deletions in order to be released
            self._attr_locks.update(multi_insert)

    async def prepare(self):
        connector = self.connector
        dumps = connector.persist.dumps

        if self.connector.server.debug:
            # check for any non persisted modifications
            desc_locator = utils.locate_descriptor_by_storage_key
            for i in self._items.values():
                if i.__snapshot__:
                    for storage_key in i.__snapshot__:
                        desc = desc_locator(type(i), storage_key)
                        if desc.get_value(i) != desc.get_value(i, False):
                            log.warn('Detected uncommitted change '
                                     f'to {desc.name} of {i.friendly_name}')

        inserted_items = []
        modified_items = []

        # expiry map
        expiry_map = {}

        # insertions
        insertions = []
        for item_id, item in self._items.items():
            expiry_map[item_id] = await item.ttl
            if item.__is_new__:
                inserted_items.append(item)
                values = dumps(item)
                insertions.append(
                    libsql_client.Statement(
                        'insert into items values '
                        f'({",".join(["?"] * len(values.keys()))})',
                        values.values()
                    )
                    # Insertion('items', dumps(item), expiry_map[item_id],
                    #           Formats.JSON)
                )
            else:
                modified_items.append(item)

        # update insertions with externals
        # for key, doc in self._ext_insertions.items():
        #     value = doc.value
        #     if value:
        #         insertions.append(
        #             Insertion(key, value, expiry_map[doc.item_id],
        #                       Formats.guess_format(value))
        #         )

        rest_ops = []

        # sub-document mutations
        # for item_id, mutations in self._sd.items():
        #     if item_id not in self._deletions:
        #         rest_ops.append(SubDocumentMutation(item_id, mutations))

        # binary appends
        auto_splits = []
        # if self._appends:
        #     split_threshold = connector.coll_split_threshold
        #     rnd = random.random()
        #     for item_id, appends in self._appends.items():
        #         ttl = expiry_map[item_id]
        #         for k, v in appends.items():
        #             if k not in self._deletions:
        #                 append = ''.join(v)
        #                 rest_ops.append(
        #                     Append(k, append, ttl, Formats.STRING)
        #                 )
        #                 possibility = len(append) / split_threshold
        #                 if rnd <= possibility * 1.8:
        #                     auto_splits.append((k, ttl))

        # upsertions
        for key, doc in self._ext_upsertions.items():
            value = doc.value
            if value:
                rest_ops.append(
                    Upsertion(key, value, expiry_map[doc.item_id],
                              Formats.guess_format(value))
                )

        # deletions
        deleted_items = [
            item for item in self._deletions.values()
            if item is not None
        ]
        rest_ops.extend([Deletion(key) for key in self._deletions])
        # remove locks
        rest_ops.extend([Deletion(key) for key in self._attr_locks])

        return (
            (insertions, rest_ops, auto_splits),
            (inserted_items, modified_items, deleted_items)
        )

    async def commit(self) -> None:
        """
        Commits the transaction.

        @return: None
        """
        # prepare
        db_ops, affected_items = await self.prepare()
        insertions, rest_ops, auto_splits = db_ops
        # print(rest_ops)
        connector = self.connector

        # insertions
        if insertions:
            # first transaction phase - make sure all keys are non-existing
            # await self.insert_multi(insertions)
            await connector.db.batch(insertions)

        if rest_ops:
            errors = await connector.batch_update(rest_ops)
            if any(errors):
                # some updates have failed
                for exc in (e for e in errors if e is not None):
                    log.error(f'Transaction.commit: {exc}')

        self.connector.active_txns -= 1
        self._committed = True

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

        if auto_splits:
            schema_service = get_service('schema')
            for key, ttl in auto_splits:
                await schema_service.auto_split(key, ttl)

        ctx_txn.set(None)

    @staticmethod
    async def _exec_post_handler(handler: str, items: list, actor):
        async with context_user(server.system_user):
            tasks = [getattr(item, handler)(actor) for item in items]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [result if isinstance(result, Exception) else None
                      for result in results]
            if any(errors):
                message = \
                    'Uncaught exception in post {0} handler of type {1}\n{2}'
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
        if not self._committed:
            # release attribute locks
            if self._attr_locks:
                unlocks = [Deletion(key) for key in self._attr_locks]
                errors = await self.connector.batch_update(unlocks)
                if any(errors):
                    # some updates have failed
                    for exc in (e for e in errors if e is not None):
                        log.error(f'Transaction.abort: {exc}')
            self.connector.active_txns -= 1
        ctx_txn.set(None)
