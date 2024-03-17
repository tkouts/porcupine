from collections import OrderedDict
from typing import Tuple

from porcupine import db
from porcupine.hinting import TYPING
from porcupine.exceptions import Forbidden, ContainmentError, NotFound
from porcupine.core.context import system_override, context
from porcupine.core.schema.storage import UNSET
from .asyncsetter import AsyncSetterValue
from porcupine.connectors.postgresql.query import QueryType
from pypika import Parameter, Query, Order
from pypika.terms import ValueWrapper
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

    def query(self, query_type=QueryType.ITEMS, where=None,
              order_by=None, order=Order.asc):
        q = self._desc.query(query_type)
        q.set_params(self.__query_params)
        _db = context.db
        if _db.txn is not None:
            # READ YOUR OWN WRITES
            instance, descr = self._inst(), self._desc
            removals = _db.txn.get_collection_removals(descr, instance)
            if removals:
                q = q.where(self.id.notin(removals))
            additions = _db.txn.get_collection_additions(descr, instance)
            if additions:
                dumps = _db.persist.dumps
                for added_item in additions:
                    values = dumps(added_item, read_uncommitted=True)
                    q *= Query.select(
                        *[ValueWrapper(i) for i in (values.values())]
                    )
                # print(q._q)
        if where is not None:
            q = q.where(where)
        if order_by is not None:
            q = q.orderby(order_by, order=order)
        return q

    @property
    def __membership_check_query(self):
        return self.query(
            QueryType.RAW,
            where=self.id == Parameter(':member_id')
        ).select(1)

    def is_fetched(self) -> bool:
        instance = self._inst()
        return instance.__is_new__ or self._desc.get_value(
            instance, use_default=False
        ) is not UNSET

    def items(
        self,
        skip=0,
        take=None,
        where=None,
        order_by=None,
        order=Order.asc,
        resolve_shortcuts=False,
        **kwargs
    ):
        q = self.query(where=where, order_by=order_by, order=order)
        return q.cursor(skip, take, resolve_shortcuts, **kwargs)

    async def ids(self) -> Tuple[TYPING.ITEM_ID]:
        if not self.is_fetched():
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
                    context.db.txn.mutate_collection(
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
                    if not item.__is_new__:
                        await context.db.txn.upsert(item)
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
                    context.db.txn.mutate_collection(
                        assoc_table.get_table_name(),
                        0,
                        values
                    )
                else:
                    with system_override():
                        setattr(item, descriptor.rel_attr, None)
                    if not item.__is_new__:
                        await context.db.txn.upsert(item)

    async def reset(self, value: list):
        # print('reset', value)
        descriptor, instance = self._desc, self._inst()
        # TODO: remove this after all calls are fixed
        if value and type(value[0]) == str:
            value = await db.get_multi(value, quiet=False).list()
        # remove collection appends
        # context.txn.reset_key_append(descriptor.key_for(instance))
        if not self.is_fetched():
            # fetch value from db
            setattr(
                instance.__externals__,
                descriptor.storage_key,
                await self.items().list()
            )
        await super().reset(value)

        old_value = descriptor.get_value(instance, snapshot=False)

        new_ids = set([i.id for i in value])
        old_ids = set([i.id for i in old_value])
        added_ids = new_ids.difference(old_ids)
        removed_ids = old_ids.difference(new_ids)
        added = [i for i in value if i.id in added_ids]
        removed = [i for i in old_value if i.id in removed_ids]

        with system_override():
            await self.add(*added)
            await self.remove(*removed)

        # super(List, descriptor).__set__(instance, value)
        return added, removed

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
                    break
        if member is None and not quiet:
            raise NotFound(
                f"Collection '{self._desc.name} '"
                f" has no member with ID '{item_id}'."
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
            return await self.get_member_by_id(item_id) is not None

    async def count(self, where=None):
        q = self.query(QueryType.RAW, where).select(Count(1))
        return (await q.execute(first_only=True))[0]
