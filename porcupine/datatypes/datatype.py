import copy
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
        self.default = default
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
                raise TypeError('Attribute "{0}" of "{1}" is readonly'.format(
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
        name = self.name
        storage = getattr(instance, self.storage)
        if name in storage:
            return storage[name]
        else:
            if isinstance(self.default, (list, dict)):
                # mutable
                value = copy.deepcopy(self.default)
                storage[name] = value
                return value
            else:
                return self.default

    def __set__(self, instance, value):
        self.validate_value(value, instance)
        self.snapshot(instance, value)
        getattr(instance, self.storage)[self.name] = value

    def __delete__(self, instance):
        if self.name in instance.__storage__:
            del instance.__storage__[self.name]

    def snapshot(self, instance, value):
        if self.name not in instance.__snapshot__:
            previous_value = getattr(instance, self.storage).get(self.name)
            if previous_value != value:
                if not instance.__snapshot__:
                    # first mutation
                    context.txn.update(instance)
                instance.__snapshot__[self.name] = previous_value

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
        pass

    def on_delete(self, instance, value, is_permanent):
        pass

    def on_undelete(self, instance, value):
        pass
