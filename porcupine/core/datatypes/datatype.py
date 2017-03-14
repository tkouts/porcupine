import copy
import collections
from porcupine import context


class DataType:
    """
    Base data type class.

    Use this as a base class if you want to create your own custom data type.
    """
    required = False
    allow_none = False
    readonly = False
    safe_type = object
    storage = '__storage__'

    def __init__(self, default=None, **kwargs):
        self._default = default
        self.name = None
        if 'required' in kwargs:
            self.required = kwargs['required']
        if 'allow_none' in kwargs:
            self.allow_none = kwargs['allow_none']
        if 'readonly' in kwargs:
            self.readonly = kwargs['readonly']
        self.validate_value(default, None)

    def validate_value(self, value, instance):
        if instance is not None:
            if self.readonly and not instance.__is_new__ \
                    and not context.is_system_update:
                raise AttributeError(
                    'Attribute {0} of {1} is readonly'.format(
                        self.name, instance.__class__.__name__))
        if self.allow_none and value is None:
            return
        if not isinstance(value, self.safe_type):
            raise TypeError('Unsupported type "{}" for "{}"'.format(
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
            raise ValueError('Attribute "{}" of "{}" is mandatory.'.format(
                self.name, instance.__class__.__name__))

    def clone(self, instance, memo):
        pass

    def on_change(self, instance, value, old_value):
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
