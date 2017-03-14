"""
Porcupine data types
====================
"""
import hashlib

from .datatype import DataType


class String(DataType):
    """String data type"""
    safe_type = str

    def __init__(self, default='', **kwargs):
        super().__init__(default, **kwargs)


class Integer(DataType):
    """Integer data type

    @ivar value: The datatype's value
    @type value: int
    """
    safe_type = int

    def __init__(self, default=0, **kwargs):
        super().__init__(default, **kwargs)


class Float(DataType):
    """Float data type

    @ivar value: The datatype's value
    @type value: float
    """
    safe_type = float

    def __init__(self, default=0.0, **kwargs):
        super().__init__(default, **kwargs)


class Boolean(DataType):
    """Boolean data type

    @ivar value: The datatype's value
    @type value: bool
    """
    safe_type = bool

    def __init__(self, default=False, **kwargs):
        super().__init__(default, **kwargs)


class Date(String):
    """Date data type"""
    allow_none = True

    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)

    # def __get__(self, instance, owner):
    #     if instance is None:
    #         return self
    #     value = super(Date, self).__get__(instance, owner)
    #     if value is not None:
    #         return date.Date(value)
    #
    # def __set__(self, instance, value):
    #     if isinstance(value, date.Date):
    #         value = value.value
    #     super(Date, self).__set__(instance, value)


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
        super().__set__(instance, hashlib.md5(value).hexdigest())
        # self.validate_value(value)
        # instance.__storage__[self.name] = hashlib.md5(value).hexdigest()

    def validate(self, instance):
        if self.required \
                and self.__get__(instance, None) == self.empty:
            raise ValueError('Attribute "{}" of "{}" is mandatory.'.format(
                self.name, instance.__class__.__name__))
