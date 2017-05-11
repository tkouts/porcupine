from porcupine import db, context
from porcupine.exceptions import ContainmentError, NotFound, Forbidden, \
    InvalidUsage, AttributeSetError
from porcupine.core.services.schema import SchemaMaintenance
from porcupine.core.context import system_override
from porcupine.contract import contract
from porcupine.utils import system, permissions
from .external import Text
from .common import String


class Acceptable:
    accepts = ()

    def __init__(self, **kwargs):
        self.accepts_resolved = False
        if 'accepts' in kwargs:
            self.accepts = kwargs['accepts']

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            self.accepts = tuple([
                system.get_content_class(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
            self.accepts_resolved = True
        return self.accepts

    async def accepts_item(self, item) -> bool:
        return isinstance(item, self.allowed_types)


class ItemReference(str):
    async def item(self):
        """
        This method returns the object that this data type
        instance references. If the current user has no read
        permission on the referenced item or it has been deleted
        then it returns None.

        @rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
        @return: The referenced object, otherwise None
        """
        return await db.get_item(self)


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
            try:
                ref_item = await db.get_item(value, quiet=False)
            except (NotFound, Forbidden):
                # TODO: change wording
                raise InvalidUsage('Invalid item {0}'.format(value))
            if not await self.accepts_item(ref_item):
                raise ContainmentError(instance, self.name, ref_item)
            return ref_item

    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
        return await self.on_create(instance, value)

    def clone(self, instance, memo):
        value = super().__get__(instance, None)
        super().__set__(instance, memo['_id_map_'].get(value, value))

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        value = getattr(instance, self.name)
        if expand:
            return await value.item()
        return value


class ItemCollection:
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor, instance):
        self._desc = descriptor
        self._inst = instance

    async def get(self):
        storage = getattr(self._inst, self._desc.storage)
        name = self._desc.name
        if getattr(storage, name) is None:
            await self._desc.fetch(self._inst)
        return tuple(getattr(storage, name))

    async def get_item_by_id(self, item_id):
        ids = await self.get()
        if item_id in ids:
            return await db.get_item(item_id, quiet=False)

    async def items(self):
        return await db.get_multi(await self.get())

    async def _check_permissions_and_raise(self):
        user = context.user
        user_role = await permissions.resolve(self._inst, user)
        if user_role < permissions.AUTHOR:
            raise Forbidden('Forbidden')

    async def add(self, *items):
        if not context.system_override:
            await self._check_permissions_and_raise()
        collection_key = self._desc.key_for(self._inst)
        for item in items:
            item_id = self._desc.get_collection_id(item)
            if not await self._desc.accepts_item(item):
                raise ContainmentError(self._inst, self._desc.name, item)
            if self._inst.__is_new__:
                storage = getattr(self._inst, self._desc.storage)
                collection = getattr(storage, self._desc.name)
                if item_id not in collection:
                    collection.append(item_id)
            else:
                context.txn.append(collection_key, ' {0}'.format(item_id))

    async def remove(self, *items):
        if not context.system_override:
            await self._check_permissions_and_raise()
        collection_key = self._desc.key_for(self._inst)
        for item in items:
            item_id = self._desc.get_collection_id(item)
            if self._inst.__is_new__:
                storage = getattr(self._inst, self._desc.storage)
                collection = getattr(storage, self._desc.name)
                if item_id in collection:
                    collection.remove(item_id)
            else:
                context.txn.append(collection_key, ' -{0}'.format(item_id))


class ReferenceN(Text, Acceptable):
    storage_info = '_refN_'
    safe_type = (list, tuple)
    allow_none = False

    def __init__(self, default=(), **kwargs):
        if 'required' in kwargs:
            raise TypeError(
                self.type_error_message.format(type(self).__name__, 'required'))
        super().__init__(default, **kwargs)
        Acceptable.__init__(self, **kwargs)

    @staticmethod
    def get_collection_id(item):
        return item.__storage__.id

    async def fetch(self, instance, set_storage=True):
        chunks = []
        current_size = 0
        is_split = False
        value = await super().fetch(instance, set_storage=False)
        # print('raw value is', value)
        if value:
            current_size = len(value)
            chunks.append(value)

        active_chunk_key = system.get_active_chunk_key(self.name)
        active_index = getattr(instance.__storage__, active_chunk_key)
        if active_index > 0:
            # collection is split
            is_split = True
            coll_key = self.key_for(instance)
            previous_chunks, _ = await system.fetch_collection_chunks(coll_key)
            # add previous chunks to the beginning
            chunks[0:0] = previous_chunks

        value, dirtiness = system.resolve_set(' '.join(chunks))
        # print(dirtiness)
        split_threshold = db.connector.coll_split_threshold
        compact_threshold = db.connector.coll_compact_threshold
        if current_size > split_threshold or dirtiness > compact_threshold:
            # collection maintenance
            key = self.key_for(instance)
            if current_size > split_threshold:
                shd_compact = (current_size * (1 - dirtiness)) < split_threshold
                if not is_split and shd_compact:
                    await SchemaMaintenance.compact_collection(key)
                else:
                    await SchemaMaintenance.rebuild_collection(key)
            elif dirtiness > compact_threshold:
                if is_split:
                    await SchemaMaintenance.rebuild_collection(key)
                else:
                    await SchemaMaintenance.compact_collection(key)
        if set_storage:
            storage = getattr(instance, self.storage)
            setattr(storage, self.storage_key, value)
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return ItemCollection(self, instance)

    def set_default(self, instance, value=None):
        if value is None:
            value = self._default
        if isinstance(value, tuple):
            value = list(value)
        super().set_default(instance, value)
        # add active key index
        active_chunk_key = system.get_active_chunk_key(self.name)
        setattr(instance.__storage__, active_chunk_key, 0)

    def key_for(self, instance, chunk=None):
        if chunk is None:
            # return active chunk
            active_chunk_key = system.get_active_chunk_key(self.name)
            chunk = getattr(instance.__storage__, active_chunk_key)
        return system.get_collection_key(instance.id, self.name, chunk)

    async def clone(self, instance, memo):
        ids = await getattr(instance, self.name).get()
        self.__set__(instance, [memo['_id_map_'].get(oid, oid) for oid in ids])

    async def on_create(self, instance, value):
        if value:
            ref_items = await db.get_multi(value)
            # check containment
            for item in ref_items:
                if not await self.accepts_item(item):
                    raise ContainmentError(instance, self.name, item)
            if ref_items:
                # write external
                raw_value = ' '.join([self.get_collection_id(i)
                                      for i in ref_items])
                super().on_create(instance, raw_value)
        else:
            ref_items = []
        return ref_items, []

    async def on_change(self, instance, value, old_value):
        # old_value is always None
        # need to compute deltas
        old_ids = await self.fetch(instance, set_storage=False)
        new_value = frozenset(value)
        if new_value == frozenset(old_ids):
            # nothing changed
            return [], []
        # compute old value leaving out non-accessible items
        ref_items = await db.get_multi(old_ids)
        old_value = frozenset([i.id for i in ref_items])
        added_ids = new_value.difference(old_value)
        removed_ids = old_value.difference(new_value)
        added = await db.get_multi(added_ids)
        removed = await db.get_multi(removed_ids)
        item_collection = self.__get__(instance, None)
        with system_override():
            await item_collection.add(*added)
            await item_collection.remove(*removed)
        return added, removed

    async def on_delete(self, instance, value):
        super().on_delete(instance, value)
        active_chunk_key = system.get_active_chunk_key(self.name)
        active_chunk = getattr(instance.__storage__, active_chunk_key) - 1
        if active_chunk > -1:
            while True:
                external_key = system.get_collection_key(instance.id,
                                                         self.name,
                                                         active_chunk)
                _, key_exists = await db.connector.exists(external_key)
                if not key_exists:
                    break
                context.txn.delete_external(external_key)
                active_chunk -= 1

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        collection = getattr(instance, self.name)
        if expand:
            return await collection.items()
        return await collection.get()

    @contract(accepts=str)
    @db.transactional()
    async def post(self, instance, request):
        """
        Adds an item to the collection
        :param instance: 
        :param request: 
        :return: 
        """
        item = await db.get_item(request.json, quiet=False)
        collection = getattr(instance, self.name)
        try:
            await collection.add(item)
        except AttributeSetError as e:
            raise InvalidUsage(str(e))
        return True
