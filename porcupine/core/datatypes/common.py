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
    """Integer data type"""
    safe_type = int

    def __init__(self, default=0, **kwargs):
        super().__init__(default, **kwargs)


class Float(DataType):
    """Float data type"""
    safe_type = float

    def __init__(self, default=0.0, **kwargs):
        super().__init__(default, **kwargs)


class Boolean(DataType):
    """Boolean data type"""
    safe_type = bool

    def __init__(self, default=False, **kwargs):
        super().__init__(default, **kwargs)


class Password(String):
    """Password data type"""
    empty = hashlib.sha3_256(b'').hexdigest()
    protected = True

    def __set__(self, instance, value):
        digest = hashlib.sha3_256(value.encode('utf-8')).hexdigest()
        super().__set__(instance, digest)

    def validate(self, value):
        if self.required and (not value or value == self.empty):
            raise ValueError('Attribute {0} is mandatory'.format(self.name))
