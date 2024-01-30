from porcupine import db, exceptions
from porcupine.contract import contract
from porcupine.core.context import context, system_override
from porcupine.core.services import db_connector
from porcupine.core.schema.storage import UNSET
from porcupine.core.schemaregistry import get_content_class
from porcupine.core import utils
from .collection import ItemCollection
# from .datatype import DataType
from .common import String
from .mutable import List
# from .external import Text, Blob
from .asyncsetter import AsyncSetter
from pypika import Table, Field


class Acceptable:

    def __init__(self, accepts, cascade_delete):
        self.accepts_resolved = False
        self.accepts = accepts
        self.cascade_delete = cascade_delete

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            self.accepts = tuple([
                get_content_class(x) if isinstance(x, str) else x
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
        permission on the referenced item, or it has been deleted
        then it returns None.

        @rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
        @return: The referenced object, otherwise None
        """
        return await db.get_item(self, quiet=quiet)

    def to_json(self):
        return str(self)


class Reference1(String, Acceptable):
    """
    This data type is used whenever an item loosely references
    at most one other item. Using this data type, the referenced item
    B{IS NOT} aware of the items that reference it.
    """
    def __init__(self, default=None, accepts=(), cascade_delete=False,
                 **kwargs):
        super().__init__(default, allow_none=True, **kwargs)
        Acceptable.__init__(self, accepts, cascade_delete)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None:
            return ItemReference(value)

    async def on_create(self, instance, value):
        await super().on_create(instance, value)
        if value:
            ref_item = await db_connector().get(value)
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
        await super().on_delete(instance, value)
        if value and self.cascade_delete:
            ref_item = await db_connector().get(value)
            if ref_item:
                with system_override():
                    await ref_item.remove()

    async def on_recycle(self, instance, value):
        await super().on_recycle(instance, value)
        if value and self.cascade_delete:
            ref_item = await db_connector().get(value)
            if ref_item:
                with system_override():
                    # mark as deleted
                    ref_item.is_deleted += 1
                    await context.txn.upsert(ref_item)
                    await context.txn.recycle(ref_item)

    async def on_restore(self, instance, value):
        await super().on_restore(instance, value)
        if value and self.cascade_delete:
            ref_item = await db_connector().get(value)
            if ref_item:
                with system_override():
                    await ref_item.restore()

    def clone(self, instance, memo):
        value = super().__get__(instance, None)
        super().__set__(instance, memo['_id_map_'].get(value, value))

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        value = getattr(instance, self.name)
        if expand:
            return await value.item()
        return value


class ReferenceN(AsyncSetter, List, Acceptable):
    # storage_info = '_refN_'
    safe_type = list, tuple
    # allow_none = False
    storage = '__externals__'
    # t = Table('items')
    columns = (
        'id', 'sig', 'type', 'name', 'created',
        'modified', 'is_collection', 'is_system', 'acl',
        'parent_id', 'p_type', 'expires_at', 'is_deleted'
    )
    # star_fields = (
    #     'id', 'sig', 'type', 'name', 'created',
    #     'modified', 'is_system', 'acl',
    #     'parent_id', 'expires_at', 'is_deleted', 'data'
    # )

    def __init__(self, default=(), accepts=(), cascade_delete=False, **kwargs):
        super(List, self).__init__(default, allow_none=False,
                                   store_as=None, **kwargs)
        Acceptable.__init__(self, accepts, cascade_delete)
        self.t = Table('items')
        self.data_field = self.t.field('data')

    def getter(self, instance, value=None):
        return ItemCollection(self, instance)

    # def current_chunk(self, instance) -> int:
    #     active_chunk_key = utils.get_active_chunk_key(self.name)
    #     current_chunk = getattr(instance.__storage__, active_chunk_key)
    #     if current_chunk is UNSET:
    #         return 0
    #     return current_chunk

    # def key_for(self, instance, chunk=None):
    #     if chunk is None:
    #         chunk = self.current_chunk(instance)
    #     return utils.get_collection_key(instance.id, self.name, chunk)

    async def clone(self, instance, memo):
        collection = getattr(instance, self.name)
        super(List, self).__set__(instance, [memo['_id_map_'].get(oid, oid)
                                             async for oid in collection])

    # allow regular snapshots
    # snapshot = DataType.snapshot

    # permissions providers

    @staticmethod
    async def can_add(instance, *items):
        return await instance.can_update(context.user)

    can_remove = can_add

    # Event handlers

    async def on_create(self, instance, value):
        # print('create', self.name, value)
        if value:
            ref_items = await db.get_multi(value).list()
            # ref_items = [
            #     item async for item in db_connector().get_multi(value)
            # ]
        else:
            ref_items = []
        await super().on_create(instance, [i.__storage__.id for i in ref_items])
        collection = self.__get__(instance, None)
        try:
            await collection.add(*ref_items)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return ref_items, []

    async def on_change(self, instance, value, old_value):
        # print('change', value, old_value)
        # need to compute deltas
        collection = self.__get__(instance, None)
        new_value = set(value)
        # compute old value leaving out non-accessible items
        ref_items = await db.get_multi(old_value).list()
        old_value = set([i.__storage__.id for i in ref_items])
        added_ids = new_value.difference(old_value)
        removed_ids = old_value.difference(new_value)
        added = await db.get_multi(added_ids).list()
        removed = await db.get_multi(removed_ids).list()
        try:
            await collection.add(*added)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await collection.remove(*removed)
        return added, removed

    async def on_delete(self, instance, value):
        if self.cascade_delete:
            collection = self.__get__(instance, None)
            with system_override():
                async for ref_item in collection.items():
                    await ref_item.remove()

        await super().on_delete(instance, value)

        previous_chunk = self.current_chunk(instance) - 1
        if previous_chunk > -1:
            connector = db_connector()
            while True:
                external_key = utils.get_collection_key(instance.id,
                                                        self.name,
                                                        previous_chunk)
                _, key_exists = await connector.exists(external_key)
                if not key_exists:
                    break
                context.txn.delete_external(external_key)
                previous_chunk -= 1

    async def on_recycle(self, instance, value):
        await super().on_recycle(instance, value)
        collection = self.__get__(instance, None)
        if self.cascade_delete:
            with system_override():
                async for ref_item in collection.items():
                    # mark as deleted
                    ref_item.is_deleted += 1
                    await context.txn.upsert(ref_item)
                    await context.txn.recycle(ref_item)

    async def on_restore(self, instance, value):
        await super().on_restore(instance, value)
        collection = self.__get__(instance, None)
        if self.cascade_delete:
            with system_override():
                async for ref_item in collection.items():
                    await ref_item.restore()

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
                return await collection.items().list()
            return await collection.ids()

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

    @contract(accepts=list)
    @db.transactional()
    async def put(self, instance, request):
        """
        Adds an item to the collection
        :param instance:
        :param request:
        :return:
        """
        collection = await super().put(instance, request)
        return await collection.ids()

    @db.transactional()
    async def delete(self, instance, request):
        member_id = self.get_member_id(instance, request.path)
        collection = getattr(instance, self.name)
        if not member_id:
            raise exceptions.MethodNotAllowed('Method not allowed')
        member = await collection.get_item_by_id(member_id, quiet=False)
        await collection.remove(member)
