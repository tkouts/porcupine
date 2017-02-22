"""
Porcupine data types
====================
"""
import hashlib
import copy

# from porcupine.utils import date


class DataType(object):
    """
    Base data type class.

    Use this as a base class if you want to create your own custom data type.

    @var required: boolean indicating if the data type is mandatory
    @type required: bool
    """
    event_handler = None
    required = False
    safe_type = object

    def __init__(self, default=None, **kwargs):
        self.validate_value(default)
        self.default = default
        self.name = None
        if 'required' in kwargs:
            self.required = kwargs['required']

    def validate_value(self, value):
        if not isinstance(value, self.safe_type):
            raise TypeError('Unsupported type "{}" for "{}"'.format(
                value.__class__.__name__, self.name))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        name = self.name
        if name in instance._dict['bag']:
            return instance._dict['bag'][name]
        else:
            if isinstance(self.default, (list, dict)):
                # mutable
                value = copy.deepcopy(self.default)
                instance._dict['bag'][name] = value
                return value
            else:
                return self.default

    def __set__(self, instance, value):
        self.validate_value(value)
        instance._dict['bag'][self.name] = value

    def __delete__(self, instance):
        if self.name in instance._dict['bag']:
            del instance._dict['bag'][self.name]

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
        if self.required and not self.__get__(instance, instance.__class__):
            raise ValueError('Attribute "{}" of "{}" is mandatory.'.format(
                self.name, instance.__class__.__name__))

    def clone(self, instance, memo):
        pass

    def on_create(self, instance, value):
        pass

    def on_update(self, instance, value, old_value):
        pass

    def on_delete(self, instance, value, is_permanent):
        pass

    def on_undelete(self, instance, value):
        pass


class String(DataType):
    """String data type"""
    safe_type = str

    def __init__(self, default='', **kwargs):
        super(String, self).__init__(default, **kwargs)


class Integer(DataType):
    """Integer data type

    @ivar value: The datatype's value
    @type value: int
    """
    safe_type = int

    def __init__(self, default=0, **kwargs):
        super(Integer, self).__init__(default, **kwargs)


class Float(DataType):
    """Float data type

    @ivar value: The datatype's value
    @type value: float
    """
    safe_type = float

    def __init__(self, default=0.0, **kwargs):
        super(Float, self).__init__(default, **kwargs)


class Boolean(DataType):
    """Boolean data type

    @ivar value: The datatype's value
    @type value: bool
    """
    safe_type = bool

    def __init__(self, default=False, **kwargs):
        super(Boolean, self).__init__(default, **kwargs)


class List(DataType):
    """List data type

    @ivar value: The datatype's value
    @type value: list
    """
    safe_type = list

    def __init__(self, default=None, **kwargs):
        default = default or []
        super(List, self).__init__(default, **kwargs)


class Dictionary(DataType):
    """Dictionary data type

    @ivar value: The datatype's value
    @type value: dict
    """
    safe_type = dict

    def __init__(self, default=None, **kwargs):
        if default is None:
            default = {}
        super(Dictionary, self).__init__(default, **kwargs)


class Date(Float):
    """Date data type"""
    safe_type = (type(None), float)

    def __init__(self, default=None, **kwargs):
        super(Date, self).__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super(Date, self).__get__(instance, owner)
        if value is not None:
            return date.Date(value)

    def __set__(self, instance, value):
        if isinstance(value, date.Date):
            value = value.value
        super(Date, self).__set__(instance, value)


class DateTime(Date):
    """Datetime data type"""


class Password(String):
    """
    Password data type.

    This data type is actually storing MD5 hex digests
    of the assigned string value.
    """
    empty = hashlib.md5(''.encode()).hexdigest()

    def __set__(self, instance, value):
        self.validate_value(value)
        instance._dict['bag'][self.name] = hashlib.md5(value).hexdigest()

    def validate(self, instance):
        if self.required \
                and self.__get__(instance, instance.__class__) == self.empty:
            raise ValueError('Attribute "{}" of "{}" is mandatory.'.format(
                self.name, instance.__class__.__name__))
