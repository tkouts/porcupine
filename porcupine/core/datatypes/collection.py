from collections import OrderedDict
from porcupine.hinting import TYPING
from porcupine.exceptions import Forbidden, ContainmentError
from porcupine.core.context import system_override, context
from porcupine.core.schema.storage import UNSET
from .asyncsetter import AsyncSetterValue
# from .external import Text
from .mutable import List
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

    # @property
    # def ttl(self):
    #     return self._inst().ttl

    # @property
    # def key(self):
    #     return self._desc.key_for(self._inst())

    # @staticmethod
    # def is_consistent(_):
    #     return True

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

    async def ids(self):
        if self._desc.is_many_to_many:
            q = self.query(QueryType.RAW_ASSOCIATIVE)
            q = q.select(self._desc.join_field.as_('id'))
        else:
            q = self.query(QueryType.RAW)
            q = q.select(self.id)
        results = await q.execute()
        return [r['id'] for r in results]

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        # print('adding', self._inst.id, self._desc.name, items)
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_add(instance, *items):
                raise Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            is_many_to_many = descriptor.is_many_to_many
            with system_override():
                for item in items:
                    if not await descriptor.accepts_item(item):
                        raise ContainmentError(instance, descriptor.name, item)
                    if is_many_to_many:
                        assoc_table = self._desc.associative_table
                        values = OrderedDict({
                            f: None
                            for f in self._desc.associative_table_fields
                        })
                        values[self._desc.equality_field.name] = instance.id
                        values[self._desc.join_field.name] = item.id
                        context.txn.mutate_collection(
                            assoc_table.get_table_name(),
                            1,
                            values
                        )
                    else:
                        setattr(item, descriptor.rel_attr, instance.id)
                        await context.txn.upsert(item)

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_remove(instance, *items):
                raise Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            # for item in items:
            #     item_id = item.id
            #     context.txn.append(instance.id, collection_key, f' -{item_id}')

    async def reset(self, value: list) -> None:
        # print('reset', value)
        descriptor, instance = self._desc, self._inst()
        # remove collection appends
        # context.txn.reset_key_append(descriptor.key_for(instance))
        if not self.is_fetched:
            # fetch value from db
            storage = getattr(instance, descriptor.storage)
            setattr(storage, descriptor.storage_key, await self.ids())
        # set collection
        super(List, descriptor).__set__(instance, value)

    async def has(self, item_id: TYPING.ITEM_ID) -> bool:
        # TODO: implement
        return True

    async def count(self, where=None):
        if self._desc.is_many_to_many and where is None:
            q = self.query(QueryType.RAW_ASSOCIATIVE).select(Count(1))
        else:
            q = self.query(QueryType.RAW, where).select(Count(1))
        return (await q.execute(first_only=True))[0]
