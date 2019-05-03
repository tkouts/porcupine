from collections import AsyncIterable, OrderedDict
from typing import AsyncIterator
from functools import partial
from aiostream import stream, pipe

from porcupine.hinting import TYPING
from porcupine import db, exceptions
from porcupine.core.context import context
from porcupine.core.services import get_service, db_connector
from porcupine.core.utils import get_content_class
from .asyncsetter import AsyncSetterValue
from .external import Text


class ItemCollection(AsyncSetterValue, AsyncIterable):

    @property
    def is_fetched(self) -> bool:
        return self._desc.get_value(self._inst, snapshot=False) is not None

    async def __aiter__(self) -> TYPING.ITEM_ID:
        descriptor, instance = self._desc, self._inst
        connector = db_connector()
        dirty_count = 0
        total_count = 0
        current_size = 0
        is_split = False
        removed = {}
        resolved = OrderedDict()
        dirtiness = 0.0
        current_value = descriptor.get_value(instance)

        def resolve_chunk(c: str, add_to_resolved=True):
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
                        if add_to_resolved:
                            resolved[oid] = None
                        yield oid

        # compute deltas first without adding to resolved map
        append = ''
        if context.txn is not None:
            collection_key = descriptor.key_for(instance)
            append = context.txn.get_key_append(collection_key)
            if append:
                for item_id in resolve_chunk(append, add_to_resolved=False):
                    yield item_id

        if current_value:
            # collection is small and fetched
            if append:
                # w/appends: need to recompute
                for item_id in resolve_chunk(' '.join(current_value),
                                             add_to_resolved=False):
                    yield item_id
            else:
                # no appends: as is
                for item_id in current_value:
                    yield item_id
            return

        chunk = await self._desc.fetch(instance, set_storage=False)
        if chunk:
            current_size = len(chunk)
            # print(chunk)
            for item_id in resolve_chunk(chunk):
                yield item_id

        active_index = descriptor.current_chunk(instance)
        if active_index > 0:
            # collection is split - fetch previous chunks
            is_split = True
            previous_chunk_no = active_index - 1
            while True:
                previous_chunk_key = descriptor.key_for(instance,
                                                        chunk=previous_chunk_no)
                previous_chunk = await connector.get_raw(previous_chunk_key)
                if previous_chunk is not None:
                    for item_id in resolve_chunk(previous_chunk):
                        yield item_id
                    previous_chunk_no -= 1
                else:
                    break

        if len(resolved) <= 300:
            # set storage / no snapshot
            # cache / mark as fetched
            storage = getattr(instance, descriptor.storage)
            setattr(storage, descriptor.storage_key, list(resolved.keys()))

        # compute dirtiness factor
        if total_count:
            dirtiness = dirty_count / total_count
            # print(dirtiness)

        split_threshold = connector.coll_split_threshold
        compact_threshold = connector.coll_compact_threshold
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

    async def count(self):
        n = 0
        async for _ in self:
            n += 1
        return n

    @staticmethod
    def _is_consistent(_):
        return True

    async def items(self,
                    skip=0,
                    take=None,
                    resolve_shortcuts=False) -> AsyncIterator[
                                                TYPING.ANY_ITEM_CO]:
        shortcut = get_content_class('Shortcut')
        inconsistent = []
        get_multi = partial(db.get_multi, _return_none=True)

        feeder = stream.chunks(self, 40) | pipe.flatmap(get_multi, task_limit=1)

        if skip > 0:
            feeder |= pipe.skip(skip)
        if take is not None:
            feeder |= pipe.take(take)

        async with feeder.stream() as streamer:
            async for oid, i in streamer:
                if i is not None and self._is_consistent(i):
                    if resolve_shortcuts and isinstance(i, shortcut):
                        i = await i.get_target()
                    if i is not None:
                        yield i
                else:
                    inconsistent.append(oid)

        if inconsistent:
            collection_key = self._desc.key_for(self._inst)
            schema_service = get_service('schema')
            ttl = await self._inst.ttl
            await schema_service.clean_collection(collection_key, inconsistent,
                                                  ttl)

    async def get_item_by_id(self,
                             item_id: TYPING.ITEM_ID,
                             quiet=True) -> TYPING.ANY_ITEM_CO:
        async for oid in self:
            if oid == item_id:
                return await db.get_item(item_id, quiet=quiet)
        if not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))

    async def has(self, item_id: TYPING.ITEM_ID):
        async for oid in self:
            if oid == item_id:
                return True
        return False

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst
            if not await descriptor.can_add(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            collection_key = descriptor.key_for(instance)
            ttl = await instance.ttl
            for item in items:
                if not await descriptor.accepts_item(item):
                    raise exceptions.ContainmentError(instance,
                                                      descriptor.name, item)
                item_id = item.id
                context.txn.append(collection_key, f' {item_id}', ttl)

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst
            if not await descriptor.can_remove(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            collection_key = descriptor.key_for(instance)
            ttl = await instance.ttl
            for item in items:
                item_id = item.id
                context.txn.append(collection_key, f' -{item_id}', ttl)

    async def reset(self, value: list) -> None:
        descriptor, instance = self._desc, self._inst
        # remove collection appends
        context.txn.reset_key_append(descriptor.key_for(instance))
        if not self.is_fetched:
            # fetch value from db
            storage = getattr(instance, descriptor.storage)
            setattr(storage, descriptor.storage_key,
                    [oid async for oid in self])
        # set collection
        super(Text, descriptor).__set__(instance, value)
