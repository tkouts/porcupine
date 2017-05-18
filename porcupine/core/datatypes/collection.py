import collections
from typing import AsyncIterator

from porcupine import db, exceptions, context
from porcupine.core.utils import system, permissions
from porcupine.core.services.schema import SchemaMaintenance
from .asyncsetter import AsyncSetterValue
from .external import Text


class ItemCollection(AsyncSetterValue, collections.AsyncIterable):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor, instance):
        self._desc = descriptor
        self._inst = instance

    @property
    def is_fetched(self):
        storage = getattr(self._inst, self._desc.storage)
        return getattr(storage, self._desc.storage_key) is not None

    async def __aiter__(self):
        instance = self._inst
        descriptor = self._desc
        dirty_count = 0
        total_count = 0
        current_size = 0
        is_split = False
        removed = {}
        # TODO: OrderedDict
        resolved = {}
        dirtiness = 0.0
        storage = getattr(instance, descriptor.storage)

        def resolve_chunk(c: str):
            nonlocal total_count, dirty_count, removed, resolved
            ids = reversed([e for e in c.split(' ') if e])
            for oid in ids:
                total_count += 1
                if oid.startswith('-'):
                    key = oid[1:]
                    dirty_count += 1
                    removed[key] = None
                else:
                    if oid in removed:
                        # removed
                        dirty_count += 1
                    elif oid in resolved:
                        # duplicate
                        dirty_count += 1
                    else:
                        resolved[oid] = None
                        yield oid

        if self.is_fetched:
            for item_id in getattr(storage, descriptor.storage_key):
                yield item_id
            return

        chunk = await self._desc.fetch(instance, set_storage=False)
        if chunk:
            current_size = len(chunk)
            for item_id in resolve_chunk(chunk):
                yield item_id

        active_chunk_key = system.get_active_chunk_key(descriptor.name)
        active_index = getattr(instance.__storage__, active_chunk_key)
        if active_index > 0:
            # collection is split - fetch previous chunks
            is_split = True
            previous_chunk_no = active_index - 1
            while True:
                previous_chunk_key = descriptor.key_for(instance,
                                                        chunk=previous_chunk_no)
                previous_chunk = await db.connector.get_raw(previous_chunk_key)
                if previous_chunk is not None:
                    for item_id in resolve_chunk(previous_chunk):
                        yield item_id
                else:
                    break

        # set storage
        setattr(storage, descriptor.storage_key, list(resolved.keys()))

        # compute dirtiness factor
        if total_count:
            dirtiness = dirty_count / total_count
            # print(dirtiness)

        split_threshold = db.connector.coll_split_threshold
        compact_threshold = db.connector.coll_compact_threshold
        if current_size > split_threshold or dirtiness > compact_threshold:
            # collection maintenance
            collection_key = descriptor.key_for(instance)
            if current_size > split_threshold:
                shd_compact = (current_size * (1 - dirtiness)) < split_threshold
                if not is_split and shd_compact:
                    await SchemaMaintenance.compact_collection(collection_key)
                else:
                    await SchemaMaintenance.rebuild_collection(collection_key)
            elif dirtiness > compact_threshold:
                if is_split:
                    await SchemaMaintenance.rebuild_collection(collection_key)
                else:
                    await SchemaMaintenance.compact_collection(collection_key)

    async def items(self) -> AsyncIterator:
        chunk_size = 20  # db.connector.multi_fetch_chunk_size
        chunk = []

        async for item_id in self:
            chunk.append(item_id)
            if len(chunk) > chunk_size:
                async for i in db.get_multi(chunk, return_none=True):
                    if i is None:
                        # TODO: remove stale id
                        pass
                    else:
                        yield i
                chunk = []

        if chunk:
            async for i in db.get_multi(chunk, return_none=True):
                if i is None:
                    # TODO: remove stale id
                    pass
                else:
                    yield i

    async def get_item_by_id(self, item_id, quiet=True):
        async for oid in self:
            if oid == item_id:
                return await db.get_item(item_id, quiet=quiet)
        if not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))

    async def _check_permissions_and_raise(self):
        user = context.user
        user_role = await permissions.resolve(self._inst, user)
        if user_role < permissions.AUTHOR:
            raise exceptions.Forbidden('Forbidden')

    async def add(self, *items):
        collection_key = self._desc.key_for(self._inst)
        for item in items:
            item_id = item.id
            if self._inst.__is_new__:
                storage = getattr(self._inst, self._desc.storage)
                collection = getattr(storage, self._desc.name)
                if item_id not in collection:
                    collection.append(item_id)
            else:
                if not context.system_override:
                    await self._check_permissions_and_raise()
                if not await self._desc.accepts_item(item):
                    raise exceptions.ContainmentError(self._inst,
                                                      self._desc.name, item)
                context.txn.append(collection_key, ' {0}'.format(item_id))

    async def remove(self, *items):
        collection_key = self._desc.key_for(self._inst)
        for item in items:
            item_id = item.id
            if self._inst.__is_new__:
                storage = getattr(self._inst, self._desc.storage)
                collection = getattr(storage, self._desc.name)
                if item_id in collection:
                    collection.remove(item_id)
            else:
                if not context.system_override:
                    await self._check_permissions_and_raise()
                context.txn.append(collection_key, ' -{0}'.format(item_id))

    async def reset(self, value):
        if not self.is_fetched:
            # fetch collection
            async for _ in self:
                pass
        # set collection
        super(Text, self._desc).__set__(self._inst, value)
