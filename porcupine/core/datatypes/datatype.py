from porcupine import context, db, exceptions
from porcupine.utils.system import get_key_of_unique


class DataType:
    """
    Base data type class.

    Use this as a base class if you want to create your own custom data type.
    """
    required = False
    allow_none = False
    readonly = False
    protected = False
    store_as = None
    indexed = False
    unique = False
    safe_type = object
    storage = '__storage__'

    def __init__(self, default=None, **kwargs):
        self._default = default
        self.name = None
        for arg in ('required', 'allow_none', 'readonly',
                    'protected', 'store_as', 'indexed',
                    'unique'):
            if arg in kwargs:
                setattr(self, arg, kwargs[arg])
        self.validate_value(default, None)

    @property
    def storage_key(self):
        return self.store_as or self.name

    def validate_value(self, value, instance):
        if instance is not None:
            try:
                is_system_update = context.is_system_update
            except ValueError:
                # running outside the event loop, assume yes
                is_system_update = True
            if self.readonly and not is_system_update:
                raise AttributeError(
                    'Attribute {0} of {1} is readonly'.format(
                        self.name, instance.__class__.__name__))
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
        storage = getattr(instance, self.storage)
        return getattr(storage, self.storage_key)

    def __set__(self, instance, value):
        self.validate_value(value, instance)
        storage = getattr(instance, self.storage)
        self.snapshot(instance, value, getattr(storage, self.storage_key))
        setattr(storage, self.storage_key, value)

    def __delete__(self, instance):
        if self.storage_key in instance.__storage__:
            del instance.__storage__[self.storage_key]

    def set_default(self, instance, value=None):
        if value is None:
            value = self._default
        storage = getattr(instance, self.storage)
        self.snapshot(instance, value, getattr(storage, self.storage_key))
        setattr(storage, self.storage_key, value)

    def snapshot(self, instance, new_value, previous_value):
        storage_key = self.storage_key
        if storage_key not in instance.__snapshot__:
            if previous_value != new_value:
                instance.__snapshot__[storage_key] = previous_value
        elif instance.__snapshot__[storage_key] == new_value:
            del instance.__snapshot__[storage_key]

    def validate(self, value):
        """
        Data type validation method.

        This method is called automatically for each I{DataType}
        instance attribute of an object, whenever this object
        is appended or updated.

        @raise ValidationError:
            if the data type is mandatory and value is empty.

        @return: None
        """
        if self.required and not value:
            raise ValueError(
                'Attribute {0} is mandatory.'.format(self.name))

    def clone(self, instance, memo):
        pass

    def on_change(self, instance, value, old_value):
        self.validate(value)
        if self.unique:
            unique_key = get_key_of_unique(instance.__storage__.pid,
                                           self.name,
                                           value)
            context.txn.insert_external(unique_key, instance.__storage__.id)
        if not instance.__is_new__:
            context.txn.mutate(instance, self.storage_key,
                               db.connector.SUB_DOC_UPSERT_MUT, value)

    def on_delete(self, instance, value, is_permanent):
        pass

    def on_undelete(self, instance, value):
        pass

    # HTTP views
    def get(self, instance, request):
        return getattr(instance, self.name)

    @db.transactional()
    async def put(self, instance, request):
        try:
            setattr(instance, self.name, request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await instance.update()
