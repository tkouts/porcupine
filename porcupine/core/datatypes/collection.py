from porcupine.hinting import TYPING
from porcupine import exceptions
from porcupine.core.context import context
from porcupine.core.schema.storage import UNSET
from .asyncsetter import AsyncSetterValue
from .external import Text
from porcupine.connectors.libsql.query import QueryType
from pypika.functions import Count


class ItemCollection(AsyncSetterValue):
    def __init__(self,
                 descriptor: TYPING.DT_CO,
                 instance: TYPING.ANY_ITEM_CO):
        super().__init__(descriptor, instance)
        self.__query_params = {
            'instance_id': instance.id
        }

    def __getattr__(self, item):
        return getattr(self._desc.t, item)

    def __getitem__(self, item):
        return self._desc.t[item]

    def query(self, query_type=QueryType.ITEMS, where=None):
        q = self._desc.query(query_type)
        q.set_params(self.__query_params)
        if where is not None:
            q = q.where(where)
        return q

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

    def items(
        self,
        skip=0,
        take=None,
        where=None,
        resolve_shortcuts=False,
        **kwargs
    ):
        q = self.query(where=where)
        return q.cursor(skip, take, resolve_shortcuts, **kwargs)

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_add(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            for item in items:
                if not await descriptor.accepts_item(item):
                    raise exceptions.ContainmentError(instance,
                                                      descriptor.name, item)
                # item_id = item.id
                # context.txn.append(instance.id, collection_key, f' {item_id}')

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_remove(instance, *items):
                raise exceptions.Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            # for item in items:
            #     item_id = item.id
            #     context.txn.append(instance.id, collection_key, f' -{item_id}')

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

    async def has(self, item_id: TYPING.ITEM_ID) -> bool:
        # TODO: implement
        return True

    async def count(self, where=None):
        q = self.query(QueryType.RAW, where).select(Count(1))
        return (await q.execute(first_only=True))[0]
