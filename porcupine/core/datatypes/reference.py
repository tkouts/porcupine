from porcupine import db, context, exceptions
from porcupine.utils import system
from .external import Text
from .common import String


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


class Reference1(String):
    """
    This data type is used whenever an item loosely references
    at most one other item. Using this data type, the referenced item
    B{IS NOT} aware of the items that reference it.

    @cvar relates_to: a list of strings containing all the permitted content
                    classes that the instances of this type can reference.
    """
    allow_none = True
    accepts = ()

    def __init__(self, default=None, **kwargs):
        self.safe_type_resolved = False
        if 'accepts' in kwargs:
            self.accepts = kwargs['accepts']
        super().__init__(default, **kwargs)

    def validate_value(self, value, instance):
        if not self.safe_type_resolved:
            self.safe_type = tuple([
                system.get_rto_by_name(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
            self.safe_type_resolved = True
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
    def __init__(self, descriptor, instance, accepts):
        self._descriptor = descriptor
        self._instance = instance
        self._accepts = accepts

    @property
    def key(self):
        return '{0}_{1}'.format(self._instance.id, self._descriptor.name)

    async def get(self):
        storage = getattr(self._instance, self._descriptor.storage)
        name = self._descriptor.name
        if name not in storage:
            # build set
            uniques = {}
            # if not self._instance.__is_new__:
            value = await Text.fetch(self._descriptor, self._instance)
            if value:
                for oid in value.split(' '):
                    if oid:
                        if oid.startswith('-'):
                            key = oid[1:]
                            if key in uniques:
                                del uniques[key]
                        else:
                            uniques[oid] = None
            storage[name] = list(uniques.keys())
        return tuple(storage[name])

    async def reset(self, value):
        self._descriptor.validate_value(value, self._instance)
        await self._descriptor.snapshot(self._instance, value)
        getattr(self._instance, self._descriptor.storage)[
            self._descriptor.name] = value

    async def items(self):
        return await db.get_multi(await self.get())

    def accepts(self, item):
        if self._accepts and context.user.id != 'system':
            return isinstance(item, self._accepts)
        return True

    def add(self, item):
        if not self.accepts(item):
            raise exceptions.ContainmentError(self._instance,
                                              self._descriptor.name,
                                              item)
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            Text.snapshot(self._descriptor, self._instance, None)
            storage[name].append(item.id)
        else:
            context.txn.append(self.key, ' {0}'.format(item.id))

    def remove(self, item):
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            # add snapshot to trigger on_change
            Text.snapshot(self._descriptor, self._instance, None)
            storage[name].remove(item.id)
        else:
            context.txn.append(self.key, ' -{0}'.format(item.id))


class ReferenceN(Text):
    safe_type = tuple
    allow_none = False
    accepts = ()

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)
        if 'accepts' in kwargs:
            self.accepts = kwargs['accepts']
        self.accepts_resolved = False

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not self.accepts_resolved:
            self.accepts = tuple([
                system.get_rto_by_name(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
            self.accepts_resolved = True
        return ItemCollection(self, instance, self.accepts)

    def __set__(self, instance, value):
        raise TypeError(
            'ReferenceN attributes do not support direct assignment. '
            'Use the "reset" method instead.')

    async def snapshot(self, instance, value):
        if self.name not in instance.__snapshot__:
            previous_value = await self.__get__(instance, None).get()
            if previous_value != tuple(value):
                instance.__snapshot__[self.name] = previous_value

    async def on_change(self, instance, value, old_value):
        # print('onchange', value, old_value)
        if instance.__is_new__ and value:
            super().on_change(instance, ' '.join(value), None)

    async def get(self, request, instance):
        return await getattr(instance, self.name).get()
