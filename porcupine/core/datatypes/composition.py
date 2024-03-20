"""
Porcupine composition data types
================================
"""
from asyncio import Future
from typing import Optional  # , List as ListType

from porcupine.hinting import TYPING
from porcupine import db, exceptions
from porcupine.contract import contract
from porcupine.response import no_content
from porcupine.core.context import context, system_override, ctx_db
from .datatype import DataType
from .collection import ItemCollection
from .relator import RelatorN, Relator1
from porcupine.connectors.schematables import CompositesTable
from .mutable import List


class EmbeddedCollection(ItemCollection):
    __slots__ = ()

    async def get_member_by_id(self, item_id: TYPING.ITEM_ID, quiet=True):
        with system_override():
            return await super().get_member_by_id(item_id, quiet=quiet)

    async def add(self, *composites: TYPING.COMPOSITE_CO):
        await super().add(*composites)
        # composite_path = get_path(self._desc, self._inst())
        for composite in composites:
            # print(composite)
            if not composite.__is_new__:
                raise TypeError('Can only add new items to composition.')
            with system_override():
                instance = self._inst()
                composite.p_type = instance.content_class
                composite.expires_at = instance.expires_at
            await context.db.txn.insert(composite)

    async def remove(self, *composites: TYPING.COMPOSITE_CO):
        for composite in composites:
            await context.db.txn.delete(composite)
        await super().remove(*composites)

    async def reset(self, value: list):
        added, removed = await super().reset(value)
        for composite in value:
            if not composite.__is_new__ and composite not in removed:
                # update composite
                await context.db.txn.upsert(composite)


class Composition(RelatorN):
    """
    This data type is used for embedding a list composite objects to
    the assigned content type.

    @see: L{porcupine.schema.Composite}
    """
    def __init__(self, **kwargs):
        super().__init__(rel_attr='parent_id', **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return EmbeddedCollection(self, instance)

    @property
    def is_many_to_many(self):
        return False

    async def clone(self, instance, clone, memo):
        composites = self.__get__(instance, None)
        clone_composites = self.__get__(clone, None)
        with system_override():
            await clone_composites.add(
                *[await item.clone() async for item in composites.items()]
            )

    # HTTP views
    async def get(self, instance: TYPING.ANY_ITEM_CO, request, expand=True):
        return await super().get(instance, request, expand=expand)

    @contract(accepts=dict)
    @db.transactional()
    async def post(self, instance: TYPING.ANY_ITEM_CO, request):
        """
        Adds a new composite to the collection
        :param instance: 
        :param request: 
        :return: 
        """
        collection = getattr(instance, self.name)
        item_dict = request.json
        try:
            if '_type' not in item_dict:
                item_dict['_type'] = self.allowed_types[0].__name__
            composite = await instance.new_from_dict(item_dict)
            await collection.add(composite)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return composite

    @contract(accepts=list)
    @db.transactional()
    async def put(self, instance: TYPING.ANY_ITEM_CO, request):
        """
        Resets the collection
        :param instance: 
        :param request: 
        :return: 
        """
        composites = []
        collection = self.__get__(instance, None)
        for item_dict in request.json:
            composite_id = item_dict.pop('id', None)
            try:
                if composite_id:
                    composite = await collection.get_member_by_id(composite_id)
                    composite.reset()
                    await composite.apply_patch(item_dict)
                else:
                    if '_type' not in item_dict:
                        item_dict['_type'] = self.allowed_types[0].__name__
                    composite = await instance.new_from_dict(item_dict)
                composites.append(composite)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        await collection.reset(composites)
        await instance.update()
        return composites


# class EmbeddedItem(RelatorItem):
#     async def item(self, quiet=True) -> TYPING.COMPOSITE_CO:
#         with system_override():
#             return await db_connector().get(
#                 self,
#                 quiet=quiet,
#                 _table=self._desc.t.get_table_name()
#             )


class Embedded(Relator1):
    """
    This data type is used for embedding a composite object to
    another item.

    @see: L{porcupine.schema.Composite}
    """
    # safe_type = Composite
    # storage_info = '_comp1_'

    def __init__(self, swappable=True, **kwargs):
        super().__init__(rel_attr='parent_id', protected=True, **kwargs)
        accepts = self.accepts[0]
        table_name = accepts if isinstance(accepts, str) else accepts.__name__
        self.t = CompositesTable(self, name=table_name.lower())
        self.swappable = swappable

    async def fetch(self, embedded_id):
        with system_override():
            return await ctx_db.get().get(
                embedded_id,
                # quiet=quiet,
                _table=self.t.get_table_name()
            )

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = self.get_value(instance)  # super().__get__(instance, owner)
        if value is None:
            future = Future()
            future.set_result(value)
            return future
        return self.fetch(value)

    async def clone(self, instance, clone, memo):
        embedded = self.__get__(instance, None)
        composite = await embedded  # .item()
        if composite:
            self.__set__(instance, await composite.clone(memo))

    async def on_create(self, instance, composite: TYPING.COMPOSITE_CO):
        if composite is not None:
            composite_id = composite.id
            with system_override():
                setattr(composite, self.rel_attr, instance.id)
                composite.p_type = instance.content_class
                composite.expires_at = instance.expires_at
                await context.db.txn.insert(composite)
                # validate value
                await super().on_create(instance, composite_id)
            # keep composite id in snapshot
            self.snapshot(
                instance,
                composite_id,
                self.get_value(instance, snapshot=False)
            )
        else:
            # None validation
            await super().on_create(instance, None)

    async def on_change(self, instance,
                        composite: TYPING.COMPOSITE_CO,
                        old_composite_id: Optional[str]):
        # print('embedded on change', old_composite_id)
        if composite is not None:
            await self.on_create(instance, composite)
        else:
            self.snapshot(instance, None, old_composite_id)
        await DataType.on_change(
            self,
            instance,
            composite.id if composite else None,
            old_composite_id
        )
        if old_composite_id is not None:
            with system_override():
                composite = await context.db.get(
                    old_composite_id,
                    _table=self.t.get_table_name()
                )

                if composite is not None:
                    await context.db.txn.delete(composite)
            # await self.on_delete(instance, old_composite_id)

    # async def on_delete(self, instance, value: Optional[str]):
    #     if value is not None:
    #         _, composite_id = value.split(':')
    #         with system_override():
    #             composite = await db.get_item(composite_id)
    #         if composite is not None:
    #             await context.txn.delete(composite)

    # HTTP views
    # These are called when there is no embedded item

    async def get(self, instance, request, **kwargs):
        return await getattr(instance, self.name)

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, instance, request):
        """
        Creates a new composite and sets the data type
        :param instance: 
        :param request: 
        :return: 
        """
        item_dict = request.json
        try:
            embedded = await instance.new_from_dict(item_dict)
            setattr(instance, self.name, embedded)
            await instance.update()
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return embedded

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, instance, request):
        patch = request.json
        try:
            embedded = await getattr(instance, self.name)
            await embedded.apply_patch(patch)
            await embedded.update()
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return embedded

    @db.transactional()
    async def delete(self, instance, _):
        embedded = await getattr(instance, self.name)
        await embedded.remove()
        return no_content()
