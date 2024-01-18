from collections import MutableSequence, MutableMapping

import cbor

from porcupine import db, exceptions
from porcupine.core.context import context
from porcupine.core.schema.storage import UNSET
from porcupine.response import json
from porcupine.core.utils import get_key_of_unique
from porcupine.connectors.mutations import SubDocument


class DataType:
    """
    Base data type class.
    """
    safe_type = object
    storage = '__storage__'

    def __init__(self, default=None, required=False, allow_none=False,
                 readonly=False, immutable=False, protected=False,
                 store_as=None, unique=False, xform=None,
                 lock_on_update=False):
        self.default = default
        self.required = required
        self.allow_none = allow_none
        self.readonly = readonly
        self.immutable = immutable
        self.protected = protected
        self.store_as = store_as
        # self.indexed = indexed
        self.unique = unique
        self.lock_on_update = lock_on_update
        self.xform = xform
        self.name = None
        self.validate_value(None, default)

    @property
    def storage_key(self):
        return self.store_as or self.name

    @property
    def should_lock(self):
        return self.lock_on_update or self.unique

    def get_value(self, instance, snapshot=True):
        storage_key = self.storage_key
        i_snapshot = instance.__snapshot__
        if snapshot and storage_key in i_snapshot:
            # modified attr
            return i_snapshot[storage_key]
        storage = getattr(instance, self.storage)
        value = getattr(storage, storage_key)
        if value is UNSET:
            return self.default
        return value

    def validate_value(self, instance, value):
        if instance is not None:
            if not context.system_override:
                if self.readonly and value != self.get_value(instance):
                    raise AttributeError(
                        'Attribute {0} of {1} is readonly'.format(
                            self.name, type(instance).__name__))
                elif self.immutable and not instance.__is_new__:
                    storage = getattr(instance, self.storage)
                    if getattr(storage, self.storage_key) is not UNSET:
                        raise AttributeError(
                            'Attribute {0} of {1} is immutable'.format(
                                self.name, type(instance).__name__))
        if self.allow_none and value is None:
            return
        if not isinstance(value, self.safe_type):
            raise TypeError(
                'Unsupported type {0} for {1}'.format(
                    type(value).__name__,
                    self.name or type(self).__name__))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.get_value(instance)

    def __set__(self, instance, value):
        if value is not None and self.xform is not None:
            value = self.xform(value)
        self.validate_value(instance, value)
        self.snapshot(instance, value, self.get_value(instance, snapshot=False))

    def snapshot(self, instance, new_value, previous_value):
        storage_key = self.storage_key
        i_snapshot = instance.__snapshot__
        if new_value != previous_value:
            i_snapshot[storage_key] = new_value
        elif storage_key in i_snapshot:
            del i_snapshot[storage_key]

    def validate(self, value) -> None:
        """
        Data type validation method.

        This method is called automatically for each I{DataType}
        instance attribute of an object, whenever this object
        is appended or updated.

        @raise ValueError:
            if the data type is mandatory and value is empty.

        @return: None
        """
        if self.required and not value:
            raise ValueError('Attribute {0} is mandatory. '
                             'Got {1!r}.'.format(self.name, value))

    def clone(self, instance, memo):
        ...

    async def add_unique(self, instance, value):
        new_unique = get_key_of_unique(instance.parent_id,
                                       self.name,
                                       value)
        context.txn.insert_external(instance.id,
                                    new_unique,
                                    instance.id)

    def remove_unique(self, instance, value):
        unique_key = get_key_of_unique(instance.parent_id,
                                       self.name,
                                       value)
        context.txn.delete_external(unique_key)

    # event handlers

    async def on_create(self, instance, value):
        self.validate(value)
        # if self.unique and self.storage == '__storage__':
        #     await self.add_unique(instance, value)

    async def on_change(self, instance, value, old_value):
        self.validate(value)
        if self.storage == '__storage__':  # and not instance.__is_new__:
            # if self.unique:
            #     old_parent_id = instance.get_snapshot_of('parent_id')
            #     if instance.parent_id == old_parent_id:
            #         # item is not moved
            #         self.remove_unique(instance, old_value)
            #         await self.add_unique(instance, value)
            #     # else:
            #     #     # parent_id on_change handler will do the job
            if not instance.__is_new__:
                context.txn.mutate(instance,
                                   self.storage_key,
                                   SubDocument.UPSERT,
                                   value)

    async def on_delete(self, instance, value):
        ...

    async def on_recycle(self, instance, value):
        ...
        # if self.unique and self.storage == '__storage__':
        #     self.remove_unique(instance, value)

    async def on_restore(self, instance, value):
        self.validate(value)
        # if self.unique and self.storage == '__storage__':
        #     await self.add_unique(instance, value)

    # HTTP views

    def get(self, instance, request):
        return getattr(instance, self.name)

    @db.transactional()
    async def put(self, instance, request):
        try:
            await instance.apply_patch({self.name: request.json})
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await instance.update()
        return json(getattr(instance, self.name))


class MutableDataType(DataType):
    """
    Mutable data type.
    """
    @staticmethod
    def clone_value(value):
        return cbor.loads(cbor.dumps(value))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None and self.storage_key not in instance.__snapshot__:
            if isinstance(value, (MutableMapping, MutableSequence)):
                value = self.clone_value(value)
                self.snapshot(instance, value, None)
        return value

    async def on_change(self, instance, value, old_value):
        if value != old_value:
            await super().on_change(instance, value, old_value)
