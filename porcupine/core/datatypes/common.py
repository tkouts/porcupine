"""
Porcupine data types
====================
"""
import hashlib
from validate_email import validate_email

from .datatype import DataType


class String(DataType):
    """String data type"""
    safe_type = str

    def __init__(self, default='', **kwargs):
        super().__init__(default, **kwargs)


class Integer(DataType):
    """Integer data type"""
    safe_type = int
    db_cast_type = 'int'

    def __init__(self, default=0, **kwargs):
        super().__init__(default, **kwargs)


class Float(DataType):
    """Float data type"""
    safe_type = float
    db_cast_type = 'double'

    def __init__(self, default=0.0, **kwargs):
        super().__init__(default, **kwargs)


class Boolean(DataType):
    """Boolean data type"""
    safe_type = bool
    db_cast_type = 'boolean'

    def __init__(self, default=False, **kwargs):
        super().__init__(default, **kwargs)


class Password(String):
    """Password data type"""
    empty = hashlib.sha3_256(b'').hexdigest()

    def __init__(self):
        super().__init__(protected=True, required=True)

    def __set__(self, instance, value):
        digest = hashlib.sha3_256(value.encode('utf-8')).hexdigest()
        super().__set__(instance, digest)

    def validate(self, value):
        if self.required and (not value or value == self.empty):
            raise ValueError('Attribute {0} is mandatory'.format(self.name))


class Email(String):
    """Email data type"""
    def __init__(self, default='', **kwargs):
        super().__init__(default, xform=str.lower, **kwargs)

    def validate_value(self, instance, value):
        super().validate_value(instance, value)
        if value and not validate_email(value):
            raise ValueError('Invalid email')
