import copy
import collections
from porcupine import context, db
from porcupine.exceptions import InvalidUsage, ValidationError


class DataType:
    """
    Base data type class.

    Use this as a base class if you want to create your own custom data type.
    """
    required = False
    allow_none = False
    readonly = False
    protected = False
    safe_type = object
    storage = '__storage__'

    def __init__(self, default=None, **kwargs):
        self._default = default
        self.name = None
        for arg in ('required', 'allow_none', 'readonly', 'protected'):
            if arg in kwargs:
                setattr(self, arg, kwargs[arg])
        self.validate_value(default, None)

    def validate_value(self, value, instance):
        if instance is not None:
            try:
                is_system_update = context.is_system_update
            except ValueError:
                # running outside the event loop, assume yes
                is_system_update = True
            if self.readonly and not is_system_update:
                raise InvalidUsage(
                    'Attribute {0} of {1} is readonly'.format(
                        self.name, instance.__class__.__name__))
        if self.allow_none and value is None:
            return
        if not isinstance(value, self.safe_type):
            raise InvalidUsage(
                'Unsupported type {0} for {1}'.format(
                    value.__class__.__name__,
                    self.name or self.__class__.__name__))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        storage = getattr(instance, self.storage)
        return storage[self.name]

    def __set__(self, instance, value):
        self.validate_value(value, instance)
        self.snapshot(instance, value)
        storage = getattr(instance, self.storage)
        storage[self.name] = value

    def __delete__(self, instance):
        if self.name in instance.__storage__:
            del instance.__storage__[self.name]

    def set_default(self, instance):
        value = self._default
        if isinstance(value, (collections.MutableMapping,
                              collections.MutableSequence)):
            value = copy.deepcopy(value)
        elif isinstance(value, tuple):
            value = list(value)
        storage = getattr(instance, self.storage)
        if self.name not in storage:
            DataType.snapshot(self, instance, value)
            storage[self.name] = value

    def snapshot(self, instance, value):
        if self.name not in instance.__snapshot__:
            previous_value = getattr(instance, self.storage).get(self.name)
            if previous_value != value:
                instance.__snapshot__[self.name] = previous_value
        elif instance.__snapshot__[self.name] == value:
            del instance.__snapshot__[self.name]

    def validate(self, instance):
        """
        Data type validation method.

        This method is called automatically for each I{DataType}
        instance attribute of an object, whenever this object
        is appended or updated.

        @raise ValueError:
            if the data type is mandatory and is empty.

        @return: None
        """
        if self.required and not self.__get__(instance, None):
            raise ValidationError(
                'Attribute {0} of {1} is mandatory.'.format(
                    self.name, instance.__class__.__name__))

    def clone(self, instance, memo):
        pass

    def on_change(self, instance, value, old_value):
        self.validate(instance)
        if not instance.__is_new__:
            txn = context.txn
            txn.mutate(instance, self.name, txn.UPSERT_MUT, value)

    def on_delete(self, instance, value, is_permanent):
        pass

    def on_undelete(self, instance, value):
        pass

    # HTTP views
    def get(self, request, instance):
        return getattr(instance, self.name)

    @db.transactional()
    async def put(self, request, instance):
        setattr(instance, self.name, request.json)
        await instance.update()
