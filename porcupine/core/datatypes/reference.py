from porcupine import db, context, exceptions
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core import utils
from .collection import ItemCollection
from .datatype import DataType
from .common import String
from .external import Text
from .asyncsetter import AsyncSetter


class Acceptable:
    cascade_delete = False
    accepts = ()

    def __init__(self, **kwargs):
        self.accepts_resolved = False
        if 'accepts' in kwargs:
            self.accepts = kwargs['accepts']
        if 'cascade_delete' in kwargs:
            self.cascade_delete = kwargs['cascade_delete']

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            self.accepts = tuple([
                utils.get_content_class(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
            self.accepts_resolved = True
        return self.accepts

    async def accepts_item(self, item) -> bool:
        return isinstance(item, self.allowed_types)


class ItemReference(str):
    async def item(self, quiet=True):
        """
        This method returns the object that this data type
        instance references. If the current user has no read
        permission on the referenced item or it has been deleted
        then it returns None.

        @rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
        @return: The referenced object, otherwise None
        """
        return await db.get_item(self, quiet=quiet)


class Reference1(String, Acceptable):
    """
    This data type is used whenever an item loosely references
    at most one other item. Using this data type, the referenced item
    B{IS NOT} aware of the items that reference it.

    @cvar relates_to: a list of strings containing all the permitted content
                    classes that the instances of this type can reference.
    """
    allow_none = True

    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)
        Acceptable.__init__(self, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value:
            return ItemReference(value)

    async def on_create(self, instance, value):
        super().on_create(instance, value)
        if value:
            ref_item = await db.get_item(value)
            if ref_item is None:
                # TODO: change wording
                raise exceptions.InvalidUsage('Invalid item {0}'.format(value))
            if not await self.accepts_item(ref_item):
                raise exceptions.ContainmentError(instance, self.name, ref_item)
            return ref_item

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        return await self.on_create(instance, value)

    async def on_delete(self, instance, value):
        super().on_delete(instance, value)
        if value and self.cascade_delete:
            ref_item = await db.connector.get(value)
            if ref_item:
                with system_override():
                    await ref_item.remove()

    def clone(self, instance, memo):
        value = super().__get__(instance, None)
        super().__set__(instance, memo['_id_map_'].get(value, value))

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        value = getattr(instance, self.name)
        if expand:
            return await value.item()
        return value


class ReferenceN(AsyncSetter, Text, Acceptable):
    storage_info = '_refN_'
    safe_type = (list, tuple)
    allow_none = False

    def __init__(self, default=(), **kwargs):
        if 'required' in kwargs:
            raise TypeError(
                self.type_error_message.format(type(self).__name__, 'required'))
        super().__init__(default, **kwargs)
        Acceptable.__init__(self, **kwargs)

    def getter(self, instance, value=None):
        return ItemCollection(self, instance)

    def set_default(self, instance, value=None):
        if value is None:
            value = self.default
        if isinstance(value, tuple):
            value = list(value)
        super().set_default(instance, value)
        # add active key index
        active_chunk_key = utils.get_active_chunk_key(self.name)
        setattr(instance.__storage__, active_chunk_key, 0)

    def key_for(self, instance, chunk=None):
        if chunk is None:
            # return active chunk
            active_chunk_key = utils.get_active_chunk_key(self.name)
            chunk = getattr(instance.__storage__, active_chunk_key)
        return utils.get_collection_key(instance.id, self.name, chunk)

    async def clone(self, instance, memo):
        collection = getattr(instance, self.name)
        super(Text, self).__set__(instance, [memo['_id_map_'].get(oid, oid)
                                             async for oid in collection])

    # allow regular snapshots
    snapshot = DataType.snapshot

    async def on_create(self, instance, value):
        if value:
            ref_items = [i async for i in db.get_multi(value)]
            # check containment
            for item in ref_items:
                if not await self.accepts_item(item):
                    raise exceptions.ContainmentError(instance,
                                                      self.name, item)
            if ref_items:
                # write external
                raw_value = ' '.join([i.__storage__.id for i in ref_items])
                super().on_create(instance, raw_value)
        else:
            ref_items = []
        return ref_items, []

    async def on_change(self, instance, value, old_value):
        # need to compute deltas
        collection = getattr(instance, self.name)
        new_value = frozenset(value)
        # compute old value leaving out non-accessible items
        ref_items = [i async for i in db.get_multi(old_value)]
        old_value = frozenset([i.__storage__.id for i in ref_items])
        added_ids = new_value.difference(old_value)
        removed_ids = old_value.difference(new_value)
        added = [i async for i in db.get_multi(added_ids)]
        removed = [i async for i in db.get_multi(removed_ids)]
        with system_override():
            try:
                await collection.add(*added)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
            await collection.remove(*removed)
        return added, removed

    async def on_delete(self, instance, value):
        super().on_delete(instance, value)
        collection = self.__get__(instance, None)
        if self.cascade_delete:
            with system_override():
                async for ref_item in collection.items():
                    await ref_item.remove()
        active_chunk_key = utils.get_active_chunk_key(self.name)
        active_chunk = getattr(instance.__storage__, active_chunk_key) - 1
        if active_chunk > -1:
            while True:
                external_key = utils.get_collection_key(instance.id,
                                                        self.name,
                                                        active_chunk)
                _, key_exists = await db.connector.exists(external_key)
                if not key_exists:
                    break
                context.txn.delete_external(external_key)
                active_chunk -= 1

    # HTTP views

    def get_member_id(self, instance, request_path):
        chunks = request_path.split('{0}/{1}'.format(instance.id, self.name))
        member_id = chunks[-1]
        if member_id.startswith('/'):
            member_id = member_id[1:]
        return member_id

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        member_id = self.get_member_id(instance, request.path)
        collection = getattr(instance, self.name)
        if member_id:
            member = await collection.get_item_by_id(member_id, quiet=False)
            return member
        else:
            if expand:
                items = [item async for item in collection.items()]
                return items
            return [oid async for oid in collection]

    @contract(accepts=str)
    @db.transactional()
    async def post(self, instance, request):
        """
        Adds an item to the collection
        :param instance: 
        :param request: 
        :return: 
        """
        if self.get_member_id(instance, request.path):
            raise exceptions.MethodNotAllowed('Method not allowed')
        item = await db.get_item(request.json, quiet=False)
        collection = getattr(instance, self.name)
        try:
            await collection.add(item)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return True

    @db.transactional()
    async def delete(self, instance, request):
        member_id = self.get_member_id(instance, request.path)
        collection = getattr(instance, self.name)
        if not member_id:
            raise exceptions.MethodNotAllowed('Method not allowed')
        member = await collection.get_item_by_id(member_id, quiet=False)
        await collection.remove(member)
