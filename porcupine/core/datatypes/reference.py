from porcupine import db, context
from porcupine.exceptions import ContainmentError
from porcupine.utils import system
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
                system.get_rto_by_name(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
        return self.accepts

    def accepts_item(self, item):
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
        item = None
        if self:
            item = await db.get_item(self)
        return item


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

    def validate_value(self, value, instance):
        if value is not None and not self.accepts_item(value):
            raise ContainmentError(instance, self.name, value)
        super().validate_value(value, instance)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value:
            value = ItemReference(value)
        return value

    def __set__(self, instance, value):
        self.validate_value(value, instance)
        self.snapshot(instance, value.id)
        storage = getattr(instance, self.storage)
        storage[self.name] = value.id

    def clone(self, instance, memo):
        if '_id_map_' in memo:
            value = super().__get__(instance, None)
            super().__set__(instance, memo['_id_map_'].get(value, value))


class ItemCollection:
    def __init__(self, descriptor, instance):
        self._descriptor = descriptor
        self._instance = instance

    @property
    def key(self):
        return '{0}_{1}'.format(self._instance.id, self._descriptor.name)

    async def get(self):
        storage = getattr(self._instance, self._descriptor.storage)
        name = self._descriptor.name
        if name not in storage:
            await self._descriptor.fetch(self._instance)
        return tuple(storage[name])

    async def items(self):
        return await db.get_multi(await self.get())

    def add(self, item):
        if not self._descriptor.accepts_item(item):
            raise ContainmentError(self._instance,
                                   self._descriptor.name,
                                   item)
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            if item.id not in storage[name]:
                self._descriptor.snapshot(self._instance, None)
                storage[name].append(item.id)
        else:
            context.txn.append(self.key, ' {0}'.format(item.id))

    def remove(self, item):
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            if item.id in storage[name]:
                # add snapshot to trigger on_change
                self._descriptor.snapshot(self._instance, None)
                storage[name].remove(item.id)
        else:
            context.txn.append(self.key, ' -{0}'.format(item.id))


class ReferenceN(Text, Acceptable):
    safe_type = (list, tuple)
    allow_none = False

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)
        Acceptable.__init__(self, **kwargs)

    async def fetch(self, instance, set_storage=True):
        # build set
        value = await super().fetch(instance, set_storage=False)
        if value:
            value = system.resolve_set(value)
        else:
            value = []
        if set_storage:
            storage = getattr(instance, self.storage)
            storage[self.name] = value
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return ItemCollection(self, instance)

    async def on_change(self, instance, value, old_value):
        # old_value is always None
        if instance.__is_new__:
            ref_items = await db.get_multi(value)
            # check containment
            for item in ref_items:
                if not self.accepts_item(item):
                    raise ContainmentError(instance, self.name, item)
            if ref_items:
                # write external
                super().on_change(instance,
                                  ' '.join([i.id for i in ref_items]),
                                  old_value)
            return ref_items, []
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
        item_collection = getattr(instance, self.name)
        for item in added:
            item_collection.add(item)
        for item in removed:
            item_collection.remove(item)
        return added, removed

    async def get(self, request, instance, expand=False):
        expand = expand or 'expand' in request.args
        if expand:
            return await getattr(instance, self.name).items()
        return await getattr(instance, self.name).get()
