"""
Porcupine composition data types
================================
"""
from typing import Type, AsyncIterator, List

from porcupine.hinting import TYPING
from porcupine import db, exceptions, context
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core.schema.composite import Composite
from porcupine.core import utils
from .reference import ReferenceN, ItemCollection, Reference1
from .external import Text


class CompositeFactory(TYPING.COMPOSITION_TYPE):
    __slots__ = ()

    def factory(self,
                clazz: Type[TYPING.COMPOSITE_CO]=None) -> TYPING.COMPOSITE_CO:
        composite_type = clazz or self._desc.allowed_types[0]
        with system_override():
            composite = composite_type()
            parent_path = getattr(self._inst, 'path', self._inst.id)
            composite.path = utils.get_composite_path(parent_path,
                                                      self._desc.name)
        return composite


class EmbeddedCollection(ItemCollection, CompositeFactory):
    __slots__ = ()

    async def get_item_by_id(self, item_id: TYPING.ITEM_ID, quiet=True):
        with system_override():
            return await super().get_item_by_id(item_id, quiet=quiet)

    async def items(self) -> AsyncIterator[TYPING.COMPOSITE_CO]:
        with system_override():
            async for item in super().items():
                yield item

    async def add(self, *composites: TYPING.COMPOSITE_CO):
        with system_override():
            await super().add(*composites)
            for composite in composites:
                if not composite.__is_new__:
                    raise TypeError('Can only add new items to composition')
                await context.txn.insert(composite)

    async def remove(self, *composites: TYPING.COMPOSITE_CO):
        with system_override():
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
            await super().on_create(instance,
                                    [c.__storage__.id for c in value])

    async def on_change(self,
                        instance: TYPING.ANY_ITEM_CO,
                        value: List[TYPING.COMPOSITE_CO],
                        old_value: TYPING.ID_LIST):
        collection = getattr(instance, self.name)
        old_ids = frozenset(old_value)
        new_ids = frozenset([c.__storage__.id for c in value])
        removed_ids = old_ids.difference(new_ids)
        added = []
        with system_override():
            if removed_ids:
                removed = []
                get_multi = utils.multi_with_stale_resolution
                async for composite in get_multi(removed_ids):
                    removed.append(composite)
                    await context.txn.delete(composite)
                await super(EmbeddedCollection, collection).remove(*removed)
            for composite in value:
                if composite.__is_new__:
                    await context.txn.insert(composite)
                    added.append(composite)
                else:
                    await context.txn.upsert(composite)
            await super(EmbeddedCollection, collection).add(*added)

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
            composite = collection.factory()
            await composite.apply_patch(item_dict)
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
                else:
                    composite = collection.factory()
                await composite.apply_patch(item_dict)
                composites.append(composite)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        await collection.reset(composites)
        await instance.update()
        return composites


class EmbeddedItem(CompositeFactory):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor: 'Embedded', instance):
        self._desc = descriptor
        self._inst = instance

    def factory(self,
                clazz: Type[TYPING.COMPOSITE_CO]=None) -> TYPING.COMPOSITE_CO:
        composite = super().factory(clazz)
        with system_override():
            composite.id = self._desc.key_for(self._inst)
        return composite

    async def item(self, quiet=True) -> TYPING.COMPOSITE_CO:
        with system_override():
            composite_id = self._desc.key_for(self._inst)
            return await db.get_item(composite_id, quiet=quiet)


class Embedded(Reference1):
    """
    This data type is used for embedding a composite object to
    another item.

    @see: L{porcupine.schema.Composite}
    """
    safe_type = Composite
    storage_info = '_comp1_'
    storage = '__externals__'
    # TODO: disallow unique

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return EmbeddedItem(self, instance)

    def __set__(self, instance, composite):
        if composite and not composite.__is_new__:
            # TODO: revisit
            raise TypeError(
                'Can only set new composite items to {0}'.format(self.name))
        super().__set__(instance, composite)

    def set_default(self, instance, value=None):
        super().set_default(instance, value)
        # add external info
        setattr(instance.__storage__, self.name, self.storage_info)

    def key_for(self, instance):
        return utils.get_composite_path(instance.id, self.name)

    def snapshot(self, instance, composite, previous_value):
        # unconditional snapshot
        instance.__snapshot__[self.storage_key] = composite

    async def clone(self, instance, memo):
        embedded = self.__get__(instance, type(instance))
        composite = await embedded.item()
        if composite:
            self.__set__(instance, await composite.clone(memo))

    async def on_create(self, instance, composite):
        if composite is not None:
            await context.txn.insert(composite)
            with system_override():
                await super().on_create(instance, composite.__storage__.id)
        else:
            await super().on_create(instance, None)

    async def on_change(self, instance, composite, old_value):
        if composite is None:
            self.validate(None)
            await self.on_delete(instance, None)
        else:
            await self.on_create(instance, composite)

    async def on_delete(self, instance, value):
        embedded = self.__get__(instance, type(instance))
        composite = await embedded.item()
        if composite:
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
            embedded = value.factory()
            await embedded.apply_patch(item_dict)
            setattr(instance, self.name, embedded)
            await instance.update()
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return embedded

    async def delete(self, instance, request):
        # TODO:  maybe raise NotFound?
        pass
