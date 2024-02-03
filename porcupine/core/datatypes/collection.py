from collections import OrderedDict
from typing import Tuple

from porcupine import db
from porcupine.hinting import TYPING
from porcupine.exceptions import Forbidden, ContainmentError, NotFound
from porcupine.core.context import system_override, context
from porcupine.core.schema.storage import UNSET
from .asyncsetter import AsyncSetterValue
from porcupine.connectors.libsql.query import QueryType
# from porcupine.connectors.mutations import SubDocument
from pypika import Parameter
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
    def __membership_check_query(self):
        if self._desc.is_many_to_many:
            return self.query(
                QueryType.RAW_ASSOCIATIVE,
                where=self._desc.join_field == Parameter(':member_id')
            ).select(1)
        else:
            return self.query(
                QueryType.RAW,
                where=self.id == Parameter(':member_id')
            ).select(1)

    def is_fetched(self) -> bool:
        instance = self._inst()
        return instance.__is_new__ or self._desc.get_value(
            instance, use_default=False
        ) is not UNSET

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

    async def ids(self) -> Tuple[TYPING.ITEM_ID]:
        if not self.is_fetched():
            if self._desc.is_many_to_many:
                q = self.query(QueryType.RAW_ASSOCIATIVE)
                q = q.select(self._desc.join_field.as_('id'))
            else:
                q = self.query(QueryType.RAW)
                q = q.select(self.id)
            results = await q.execute()
            ids = tuple([r[0] for r in results])
            return ids
        return tuple([
            i.id for i in
            self._desc.get_value(self._inst())
        ])

    async def add(self, *items: TYPING.ANY_ITEM_CO) -> None:
        # print('adding', self._inst.id, self._desc.name, items)
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_add(instance, *items):
                raise Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            is_many_to_many = descriptor.is_many_to_many
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
                    # setattr(item.__storage__, descriptor.rel_attr, 1)
                else:
                    with system_override():
                        setattr(item, descriptor.rel_attr, instance.id)
                # if not item.__is_new__:
                #     # print('mutating counter')
                #     context.txn.mutate(instance,
                #                        descriptor.storage_key,
                #                        SubDocument.UPSERT,
                #                        1)
                    await context.txn.upsert(item)
            # update items inited flag
            # current_count = getattr(instance.__storage__, descriptor.name)
            # setattr(instance.__storage__, descriptor.name, 1)
            # if not instance.__is_new__:
            #     # print('mutating counter')
            #     context.txn.mutate(instance,
            #                        descriptor.storage_key,
            #                        SubDocument.UPSERT,
            #                        1)

    async def remove(self, *items: TYPING.ANY_ITEM_CO) -> None:
        if items:
            descriptor, instance = self._desc, self._inst()
            if not await descriptor.can_remove(instance, *items):
                raise Forbidden('Forbidden')
            await instance.touch()
            # collection_key = descriptor.key_for(instance)
            is_many_to_many = descriptor.is_many_to_many
            for item in items:
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
                        0,
                        values
                    )
                else:
                    with system_override():
                        setattr(item, descriptor.rel_attr, None)
                    await context.txn.upsert(item)

    async def reset(self, value: list) -> None:
        # print('reset', value)
        descriptor, instance = self._desc, self._inst()
        if value and type(value[0]) == str:
            value = await db.get_multi(value, quiet=False).list()
        # remove collection appends
        # context.txn.reset_key_append(descriptor.key_for(instance))
        if not self.is_fetched():
            # fetch value from db
            # storage = getattr(instance, descriptor.storage)
            setattr(
                instance.__externals__,
                descriptor.storage_key,
                await self.items().list()
            )
        await super().reset(value)
        # super(List, descriptor).__set__(instance, value)

    async def get_member_by_id(
        self,
        item_id: TYPING.ITEM_ID,
        quiet=True
    ) -> TYPING.ANY_ITEM_CO:
        member = None
        if not self.is_fetched():
            q = self.query(where=self.id == Parameter(':member_id'))
            member = await q.execute(first_only=True, member_id=item_id)
        else:
            items = self._desc.get_value(self._inst())
            for item in items:
                if item.id == item_id:
                    member = item
        if member is None and not quiet:
            raise NotFound(
                f"Collection '{self._desc.name} '"
                f"has no member with ID '{item_id}'."
            )
        return member

    async def has(self, item_id: TYPING.ITEM_ID) -> bool:
        if not self.is_fetched():
            result = await self.__membership_check_query.execute(
                first_only=True,
                member_id=item_id
            )
            return result is not None
        else:
            return self.get_member_by_id(item_id) is not None

    async def count(self, where=None):
        if self._desc.is_many_to_many and where is None:
            q = self.query(QueryType.RAW_ASSOCIATIVE).select(Count(1))
        else:
            q = self.query(QueryType.RAW, where).select(Count(1))
        return (await q.execute(first_only=True))[0]
