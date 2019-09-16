"""
Porcupine composition data types
================================
"""
from typing import Optional, AsyncIterator, List

from porcupine.hinting import TYPING
from porcupine import db, exceptions
from porcupine.contract import contract
from porcupine.core.context import context, system_override
from porcupine.core.schema.composite import Composite
from porcupine.core import utils
from porcupine.core.datatypes.asyncsetter import AsyncSetter, AsyncSetterValue
from .datatype import DataType
from .reference import ReferenceN, ItemCollection, Reference1
from .external import Text


def get_path(desc, instance):
    path = getattr(instance, 'path', instance.id)
    return utils.get_composite_path(path, desc.name)


class EmbeddedCollection(ItemCollection):
    __slots__ = ()

    async def get_item_by_id(self, item_id: TYPING.ITEM_ID, quiet=True):
        with system_override():
            return await super().get_item_by_id(item_id, quiet=quiet)

    async def items(self, resolve_shortcuts=False) -> AsyncIterator[
                                                TYPING.COMPOSITE_CO]:
        with system_override():
            async for item in super().items():
                yield item

    async def add(self, *composites: TYPING.COMPOSITE_CO):
        await super().add(*composites)
        composite_path = get_path(self._desc, self._inst)
        for composite in composites:
            if not composite.__is_new__:
                raise TypeError('Can only add new items to composition')
            # set composite path
            with system_override():
                composite.path = composite_path
            await context.txn.insert(composite)

    async def remove(self, *composites: TYPING.COMPOSITE_CO):
        await super().remove(*composites)
        for composite in composites:
            await context.txn.delete(composite)


class Composition(ReferenceN):
    """
    This data type is used for embedding a list composite objects to
    the assigned content type.

    @see: L{porcupine.schema.Composite}
    """
    storage_info = '_compN_'

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return EmbeddedCollection(self, instance)

    async def clone(self, instance, memo):
        composites = self.__get__(instance, None).items()
        super(Text, self).__set__(
            instance, [await item.clone(memo)
                       async for item in composites])

    async def on_create(self, instance, value):
        for composite in value:
            if not composite.__is_new__:
                # TODO: revisit
                raise TypeError('Can only add new items to composition')
            await context.txn.insert(composite)
        with system_override():
            await super().on_create(instance, [c.__storage__.id for c in value])

    async def on_change(self,
                        instance: TYPING.ANY_ITEM_CO,
                        composites: List[TYPING.COMPOSITE_CO],
                        old_value: TYPING.ID_LIST):
        collection = getattr(instance, self.name)
        old_ids = frozenset(old_value)
        new_ids = frozenset([c.__storage__.id for c in composites])
        removed_ids = old_ids.difference(new_ids)
        with system_override():
            added = []
            removed = []
            if removed_ids:
                async for composite in db.get_multi(removed_ids):
                    removed.append(composite)
            for composite in composites:
                if composite.__is_new__:
                    added.append(composite)
                else:
                    # update composite
                    await context.txn.upsert(composite)
            if removed:
                await collection.remove(*removed)
            if added:
                await collection.add(*added)

    async def on_delete(self, instance, value):
        collection = self.__get__(instance, None)
        async for composite in collection.items():
            await context.txn.delete(composite)
        # remove collection documents
        await super().on_delete(instance, value)

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
            if 'type' not in item_dict:
                item_dict['type'] = self.allowed_types[0].__name__
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
                    composite = await collection.get_item_by_id(composite_id)
                    composite.reset()
                    await composite.apply_patch(item_dict)
                else:
                    if 'type' not in item_dict:
                        item_dict['type'] = self.allowed_types[0].__name__
                    composite = await instance.new_from_dict(item_dict)
                composites.append(composite)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        await collection.reset(composites)
        await instance.update()
        return composites


class EmbeddedItem(AsyncSetterValue):
    async def reset(self, composite):
        await super().reset(composite)
        if composite is not None:
            if not composite.__is_new__:
                # TODO: revisit
                raise TypeError('Can only set new composite items to '
                                '{0}'.format(self._desc.name))
            # set composite path
            with system_override():
                composite.path = get_path(self._desc, self._inst)

    async def item(self, quiet=True) -> TYPING.COMPOSITE_CO:
        composite = None
        value = self._desc.get_value(self._inst)
        if value is not None:
            if isinstance(value, str):
                _, composite_id = value.split(':')
                with system_override():
                    composite = await db.get_item(composite_id, quiet=quiet)
            else:
                composite = value
        return composite


class Embedded(AsyncSetter, Reference1):
    """
    This data type is used for embedding a composite object to
    another item.

    @see: L{porcupine.schema.Composite}
    """
    safe_type = Composite
    storage_info = '_comp1_'
    # TODO: disallow unique

    def getter(self, instance, value=None):
        return EmbeddedItem(self, instance)

    def snapshot(self, instance, composite, previous_value):
        # unconditional snapshot
        instance.__snapshot__[self.storage_key] = composite

    async def clone(self, instance, memo):
        embedded = self.__get__(instance, type(instance))
        composite = await embedded.item()
        if composite:
            self.__set__(instance, await composite.clone(memo))

    async def on_create(self, instance, composite: TYPING.COMPOSITE_CO):
        if composite is not None:
            composite_id = composite.id
            await context.txn.insert(composite)
            with system_override():
                # validate value
                await super().on_create(instance, composite_id)
            # keep composite id in snapshot
            super().snapshot(instance,
                             f'{self.storage_info}:{composite_id}',
                             None)
        else:
            # None validation
            await super().on_create(instance, None)

    async def on_change(self, instance,
                        composite: TYPING.COMPOSITE_CO,
                        old_composite_id: Optional[str]):
        if composite is not None:
            await self.on_create(instance, composite)
        await DataType.on_change(
            self,
            instance,
            f'{self.storage_info}:{composite.id}' if composite else None,
            old_composite_id
        )
        if old_composite_id is not None:
            await self.on_delete(instance, old_composite_id)

    async def on_delete(self, instance, value: Optional[str]):
        if value is not None:
            _, composite_id = value.split(':')
            with system_override():
                composite = await db.get_item(composite_id)
            if composite is not None:
                await context.txn.delete(composite)

    # HTTP views
    # These are called when there is no embedded item

    async def get(self, instance, request, expand=True):
        # TODO:  maybe raise NotFound?
        return None

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, instance, request):
        """
        Creates a new composite and sets the data type
        :param instance: 
        :param request: 
        :return: 
        """
        value = self.__get__(instance, type(instance))
        item_dict = request.json
        try:
            embedded = utils.get_content_class(item_dict.pop('_type'))
            await embedded.apply_patch(item_dict)
            # setattr(instance, self.name, embedded)
            await value.reset(embedded)
            await instance.update()
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return embedded

    async def delete(self, instance, request):
        # TODO:  maybe raise NotFound?
        pass
