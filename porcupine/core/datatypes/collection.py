from typing import AsyncIterable
import weakref

from lru import LRU

from porcupine.hinting import TYPING
from porcupine import db, exceptions, pipe
from porcupine.core.context import context
from porcupine.core.services import get_service, db_connector
from porcupine.core.utils import get_content_class, get_collection_key
from porcupine.core.schema.storage import UNSET
from porcupine.core.stream.streamer import IdStreamer
from porcupine.connectors.mutations import Formats
from .asyncsetter import AsyncSetterValue
from .external import Text


class CollectionResolver:
    __slots__ = (
        'item_id',
        'collection_name',
        'chunk_no',
        'total',
        'dirty',
        'removed',
        'resolved',
        'chunk_sizes'
    )

    def __init__(self, item_id: str, name: str, chunk_no: int):
        self.item_id = item_id
        self.collection_name = name
        self.chunk_no = chunk_no
        self.total = 0
        self.dirty = 0
        self.resolved = LRU(2730)
        self.removed = set()
        self.chunk_sizes = []

    @property
    def max_chunk_size(self):
        if self.chunk_sizes:
            return max(self.chunk_sizes)
        return 0

    @property
    def is_split(self):
        return len(self.chunk_sizes) > 1

    async def __chunks(self):
        connector = db_connector()
        chunk_no = self.chunk_no
        while True:
            chunk_key = get_collection_key(self.item_id, self.collection_name,
                                           chunk_no)
            chunk = await connector.get_raw(chunk_key, fmt=Formats.STRING)
            if chunk is None:
                break
            yield chunk
            chunk_no -= 1

    def resolve_chunk(self, c: str):
        ids = reversed([e for e in c.split(' ') if e])
        for oid in ids:
            self.total += 1
            if oid.startswith('-'):
                key = oid[1:]
                self.dirty += 1
                self.removed.add(key)
            else:
                if oid in self.removed:
                    # removed
                    self.dirty += 1
                elif oid in self.resolved:
                    # duplicate
                    self.dirty += 1
                else:
                    self.resolved[oid] = True
                    yield oid

    async def __aiter__(self) -> TYPING.ITEM_ID:
        async for chunk in self.__chunks():
            self.chunk_sizes.append(len(chunk))
            for item_id in self.resolve_chunk(chunk):
                yield item_id


class CollectionIterator(AsyncIterable):
    __slots__ = '_desc', '_inst'

    def __init__(self,
                 descriptor: TYPING.DT_CO,
                 instance: TYPING.ANY_ITEM_CO):
        self._desc = descriptor
        self._inst = weakref.ref(instance)

    async def __aiter__(self) -> TYPING.ITEM_ID:
        descriptor, instance = self._desc, self._inst()
        dirtiness = 0.0
        storage = instance.__externals__
        current_value = getattr(storage, descriptor.storage_key)
        chunk_no = descriptor.current_chunk(instance)
        resolver = CollectionResolver(instance.id, descriptor.name, chunk_no)

        # compute deltas first
        append = ''
        if context.txn is not None:
            collection_key = descriptor.key_for(instance)
            append = context.txn.get_key_append(collection_key)
            if append:
                for item_id in resolver.resolve_chunk(append):
                    yield item_id

        if current_value is not UNSET:
            # collection is small and fetched
            if append:
                # w/appends: need to recompute
                for item_id in resolver.resolve_chunk(' '.join(current_value)):
                    yield item_id
            else:
                # no appends: as is
                for item_id in current_value:
                    yield item_id
            return

        total_items = 0
        collection = []

        async for item_id in resolver:
            total_items += 1
            if total_items < 301:
                collection.append(item_id)
            yield item_id

        current_size = resolver.max_chunk_size
        is_split = resolver.is_split

        if total_items < 301:
            # set storage / no snapshot
            # cache / mark as fetched
            setattr(storage, descriptor.storage_key, collection)

        # compute dirtiness factor
        if resolver.total:
            dirtiness = resolver.dirty / resolver.total

        connector = db_connector()
        split_threshold = connector.coll_split_threshold
        compact_threshold = connector.coll_compact_threshold

        # print(current_size, split_threshold)
        # print(dirtiness, compact_threshold)

        if current_size > split_threshold or dirtiness > compact_threshold:
            # collection maintenance
            collection_key = descriptor.key_for(instance)
            schema_service = get_service('schema')
            ttl = await instance.ttl
            if current_size > split_threshold:
                shd_compact = (current_size * (1 - dirtiness)) < split_threshold
                if not is_split and shd_compact:
                    await schema_service.compact_collection(collection_key, ttl)
                else:
                    await schema_service.rebuild_collection(collection_key, ttl)
            elif dirtiness > compact_threshold:
                if is_split:
                    await schema_service.rebuild_collection(collection_key, ttl)
                else:
                    await schema_service.compact_collection(collection_key, ttl)


class ItemCollection(AsyncSetterValue, IdStreamer):
    def __init__(self,
                 descriptor: TYPING.DT_CO,
                 instance: TYPING.ANY_ITEM_CO):
        AsyncSetterValue.__init__(self, descriptor, instance)
        IdStreamer.__init__(self, CollectionIterator(descriptor, instance))

    @property
    def is_fetched(self) -> bool:
        return getattr(self._inst().__externals__,
                       self._desc.storage_key) is not UNSET

    @property
    def ttl(self):
        return self._inst().ttl

    @property
    def key(self):
        return self._desc.key_for(self._inst())

    @staticmethod
    def is_consistent(_):
        return True

    @staticmethod
    async def _shortcut_resolver(i):
        shortcut = get_content_class('Shortcut')
        if isinstance(i, shortcut):
            return await i.get_target()
        return i

    def items(self, resolve_shortcuts=False):
        items = super().items(self)
        if resolve_shortcuts:
            items |= pipe.map(self._shortcut_resolver)
            items |= pipe.if_not_none()
        return items

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_add(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            collection_key = descriptor.key_for(instance)
            for item in items:
                if not await descriptor.accepts_item(item):
                    raise exceptions.ContainmentError(instance,
                                                      descriptor.name, item)
                item_id = item.id
                context.txn.append(instance.id, collection_key, f' {item_id}')

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_remove(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            collection_key = descriptor.key_for(instance)
            for item in items:
                item_id = item.id
                context.txn.append(instance.id, collection_key, f' -{item_id}')

    async def reset(self, value: list) -> None:
        descriptor, instance = self._desc, self._inst()
        # remove collection appends
        context.txn.reset_key_append(descriptor.key_for(instance))
        if not self.is_fetched:
            # fetch value from db
            storage = getattr(instance, descriptor.storage)
            setattr(storage, descriptor.storage_key,
                    [oid async for oid in self])
        # set collection
        super(Text, descriptor).__set__(instance, value)
