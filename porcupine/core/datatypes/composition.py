"""
Porcupine composition data types
================================
"""
from typing import Type, Union
from porcupine import db, exceptions, context
from porcupine.utils import system
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core.schema.composite import Composite
from .reference import ReferenceN, ItemCollection, Reference1


class CompositeFactory:
    __slots__ = ()

    def factory(self: Union['EmbeddedCollection', 'EmbeddedItem'],
                clazz: Type[Composite]=None) -> Composite:
        composite_type = clazz or self._desc.allowed_types[0]
        with system_override():
            composite = composite_type()
            parent_path = getattr(self._inst, 'path', self._inst.id)
            composite.path = system.get_composite_path(parent_path,
                                                       self._desc.name)
        return composite


class EmbeddedCollection(ItemCollection, CompositeFactory):
    __slots__ = ()

    async def get_item_by_id(self, item_id, quiet=True):
        with system_override():
            return await super().get_item_by_id(item_id, quiet=quiet)

    async def items(self):
        with system_override():
            return await super().items()

    async def add(self, *composites):
        with system_override():
            await super().add(*composites)
            for composite in composites:
                if not composite.__is_new__:
                    raise TypeError('Can only add new items to composition')
                context.txn.insert(composite)
        await self._inst.update()

    async def remove(self, *composites):
        with system_override():
            await super().remove(*composites)
            for composite in composites:
                context.txn.delete(composite)
        await self._inst.update()


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
        composites = await self.__get__(instance, None).items()
        self.__set__(instance, [await item.clone(memo) for item in composites])

    async def on_create(self, instance, value):
        for composite in value:
            if not composite.__is_new__:
                # TODO: revisit
                raise TypeError('Can only add new items to composition')
            context.txn.insert(composite)
        with system_override():
            await super().on_create(instance,
                                    [c.__storage__.id for c in value])

    async def on_change(self, instance, value, old_value):
        old_ids = frozenset(await self.fetch(instance, set_storage=False))
        collection = self.__get__(instance, None)
        new_ids = frozenset([c.__storage__.id for c in value])
        removed_ids = old_ids.difference(new_ids)
        added = []
        with system_override():
            for composite in value:
                if composite.__is_new__:
                    context.txn.insert(composite)
                    added.append(composite)
                else:
                    context.txn.upsert(composite)
            await super(EmbeddedCollection, collection).add(*added)
            if removed_ids:
                removed = await db.get_multi(removed_ids)
                for item in removed:
                    context.txn.delete(item)
                await super(EmbeddedCollection, collection).remove(*removed)

    async def on_delete(self, instance, value):
        composite_ids = await self.fetch(instance, set_storage=False)
        async for composite in db.connector.get_multi(composite_ids):
            context.txn.delete(composite)
        # remove collection documents
        await super().on_delete(instance, value)

    # HTTP views
    async def get(self, instance, request, expand=False):
        return await super().get(instance, request, expand=True)

    @contract(accepts=dict)
    @db.transactional()
    async def post(self, instance, request):
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
            composite.apply_patch(item_dict)
            await collection.add(composite)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return composite

    @contract(accepts=list)
    @db.transactional()
    async def put(self, instance, request):
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
                    composite.apply_patch(item_dict)
                else:
                    composite = collection.factory()
                    composite.apply_patch(item_dict)
                composites.append(composite)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        setattr(instance, self.name, composites)
        await instance.update()
        return composites


class EmbeddedItem(CompositeFactory):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor: 'Embedded', instance):
        self._desc = descriptor
        self._inst = instance

    def new(self, clazz: Type[Composite]=None) -> Composite:
        composite = super().factory(clazz)
        with system_override():
            composite.id = self._desc.key_for(self._inst)
        return composite

    async def item(self, quiet=True) -> Composite:
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
        return system.get_composite_path(instance.id, self.name)

    def snapshot(self, instance, composite, previous_value):
        storage_key = self.storage_key
        if storage_key not in instance.__snapshot__:
            instance.__snapshot__[storage_key] = previous_value

    async def clone(self, instance, memo):
        embedded = self.__get__(instance, None)
        composite = await embedded.item()
        if composite:
            self.__set__(instance, await composite.clone(memo))

    async def on_create(self, instance, composite):
        if composite is not None:
            context.txn.insert(composite)
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
        embedded = self.__get__(instance, None)
        composite = await embedded.item()
        if composite:
            context.txn.delete(composite)

    # HTTP views
    # These are called when there is no embedded item

    async def get(self, instance, request, expand=True):
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
        value = self.__get__(instance, None)
        item_dict = request.json
        try:
            embedded = value.factory()
            embedded.apply_patch(item_dict)
            setattr(instance, self.name, embedded)
            await instance.update()
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return embedded

    async def delete(self, instance, request):
        pass
