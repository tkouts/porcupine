from porcupine import context, db, exceptions
from porcupine.core.utils import get_key_of_unique


class DataType:
    """
    Base data type class.
    """
    required = False
    allow_none = False
    readonly = False
    immutable = False
    protected = False
    store_as = None
    indexed = False
    unique = False
    safe_type = object
    storage = '__storage__'

    def __init__(self, default=None, **kwargs):
        self.default = default
        self.name = None
        for arg in ('required', 'allow_none', 'readonly',
                    'protected', 'store_as', 'indexed',
                    'unique', 'immutable'):
            if arg in kwargs:
                setattr(self, arg, kwargs[arg])
        self.validate_value(None, default)

    @property
    def storage_key(self):
        return self.store_as or self.name

    def get_value(self, instance):
        storage = getattr(instance, self.storage)
        return getattr(storage, self.storage_key)

    def validate_value(self, instance, value):
        if instance is not None:
            if not context.system_override:
                if self.readonly and value != self.get_value(instance):
                    raise AttributeError(
                        'Attribute {0} of {1} is readonly'.format(
                            self.name, type(instance).__name__))
                elif self.immutable and not instance.__is_new__:
                    raise AttributeError(
                        'Attribute {0} of {1} is immutable'.format(
                            self.name, type(instance).__name__))
        if self.allow_none and value is None:
            return
        if not isinstance(value, self.safe_type):
            raise TypeError(
                'Unsupported type {0} for {1}'.format(
                    value.__class__.__name__,
                    self.name or self.__class__.__name__))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.get_value(instance)

    def __set__(self, instance, value):
        self.validate_value(instance, value)
        storage = getattr(instance, self.storage)
        self.snapshot(instance, value, getattr(storage, self.storage_key))
        setattr(storage, self.storage_key, value)

    def set_default(self, instance, value=None):
        if value is None:
            value = self.default
        storage = getattr(instance, self.storage)
        # add to snapshot in order to validate
        if not instance.__is_new__:
            instance.__snapshot__[self.storage_key] = value
        setattr(storage, self.storage_key, value)

    def snapshot(self, instance, new_value, previous_value):
        storage_key = self.storage_key
        if storage_key not in instance.__snapshot__:
            if previous_value != new_value:
                instance.__snapshot__[storage_key] = previous_value
        elif instance.__snapshot__[storage_key] == new_value:
            del instance.__snapshot__[storage_key]

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
        pass

    # event handlers

    def on_create(self, instance, value):
        self.validate(value)
        if self.unique and instance.__storage__.pid:
            unique_key = get_key_of_unique(instance.__storage__.pid,
                                           self.name,
                                           value)
            context.txn.insert_external(unique_key, instance.__storage__.id)

    async def on_change(self, instance, value, old_value):
        DataType.on_create(self, instance, value)
        if self.unique and instance.__storage__.pid:
            # try to lock attribute
            await context.txn.lock_attribute(instance, self.name)
            old_unique_key = get_key_of_unique(
                instance.get_snapshot_of('parent_id'), self.name, old_value)
            context.txn.delete_external(old_unique_key)
        if self.storage == '__storage__':
            context.txn.mutate(instance, self.storage_key,
                               db.connector.SUB_DOC_UPSERT_MUT, value)

    def on_delete(self, instance, value):
        if self.unique and instance.__storage__.pid:
            unique_key = get_key_of_unique(
                instance.get_snapshot_of('parent_id'),
                self.name,
                value)
            context.txn.delete_external(unique_key)

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
