from porcupine import db, context
from .external import Text, Blob


class Collection:
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
            # build set
            uniques = {}
            # if not self._instance.__is_new__:
            value = await Blob.fetch(self._descriptor, self._instance)
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

    def add(self, item_id):
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            Blob.snapshot(self._descriptor, self._instance, None)
            storage[name].append(item_id)
        else:
            context.txn.append(self.key, ' {0}'.format(item_id))

    def remove(self, item_id):
        if self._instance.__is_new__:
            storage = getattr(self._instance, self._descriptor.storage)
            name = self._descriptor.name
            # add snapshot to trigger on_change
            Blob.snapshot(self._descriptor, self._instance, None)
            storage[name].remove(item_id)
        else:
            context.txn.append(self.key, ' -{0}'.format(item_id))


class ItemCollection(Text):
    safe_type = tuple
    allow_none = False

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return Collection(self, instance)

    def __set__(self, instance, value):
        raise TypeError(
            'ItemCollection attributes do not support direct assignment. '
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
