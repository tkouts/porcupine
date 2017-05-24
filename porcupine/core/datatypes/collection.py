from collections import AsyncIterable, OrderedDict
from typing import AsyncIterator

from porcupine.hinting import TYPING
from porcupine import db, exceptions, context
from porcupine.core import utils
from porcupine.core.services.schema import SchemaMaintenance
from .asyncsetter import AsyncSetterValue
from .external import Text


class ItemCollection(AsyncSetterValue, AsyncIterable):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor: TYPING.DT_MULTI_REFERENCE_CO,
                 instance: TYPING.ANY_ITEM_CO):
        self._desc = descriptor
        self._inst = instance

    @property
    def is_fetched(self) -> bool:
        return self._desc.get_value(self._inst) is not None

    async def __aiter__(self) -> TYPING.ITEM_ID:
        instance = self._inst
        descriptor = self._desc
        dirty_count = 0
        total_count = 0
        current_size = 0
        is_split = False
        removed = {}
        resolved = OrderedDict()
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
            # print(chunk)
            for item_id in resolve_chunk(chunk):
                yield item_id

        active_chunk_key = utils.get_active_chunk_key(descriptor.name)
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
                    previous_chunk_no -= 1
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

    async def items(self) -> AsyncIterator[TYPING.ANY_ITEM_CO]:
        chunk_size = 40
        chunk = []
        get_multi = utils.multi_with_stale_resolution

        async for item_id in self:
            chunk.append(item_id)
            if len(chunk) > chunk_size:
                async for i in get_multi(chunk):
                    yield i
                chunk = []

        if chunk:
            async for i in get_multi(chunk):
                yield i

    async def get_item_by_id(self,
                             item_id: TYPING.ITEM_ID,
                             quiet=True) -> TYPING.ANY_ITEM_CO:
        async for oid in self:
            if oid == item_id:
                return await db.get_item(item_id, quiet=quiet)
        if not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst
            collection_key = descriptor.key_for(instance)
            for item in items:
                item_id = item.__storage__.id
                if instance.__is_new__:
                    storage = getattr(instance, descriptor.storage)
                    collection = getattr(storage, descriptor.name)
                    if item_id not in collection:
                        collection.append(item_id)
                else:
                    if not await descriptor.accepts_item(item):
                        raise exceptions.ContainmentError(instance,
                                                          descriptor.name, item)
                    context.txn.append(collection_key, ' {0}'.format(item_id))
            if not instance.__is_new__:
                await instance.update()

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst
            collection_key = descriptor.key_for(instance)
            for item in items:
                item_id = item.__storage__.id
                if instance.__is_new__:
                    storage = getattr(instance, descriptor.storage)
                    collection = getattr(storage, descriptor.name)
                    if item_id in collection:
                        collection.remove(item_id)
                else:
                    context.txn.append(collection_key, ' -{0}'.format(item_id))
            if not instance.__is_new__:
                await instance.update()

    async def reset(self, value: TYPING.ID_LIST) -> None:
        if not self.is_fetched:
            # fetch collection for snapshot to work
            async for _ in self:
                pass
        # TODO: reset txn appends
        # set collection
        super(Text, self._desc).__set__(self._inst, value)
