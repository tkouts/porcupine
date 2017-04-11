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


class DateTime(Date):
    """Datetime data type"""


class Password(String):
    """
    Password data type.

    This data type is actually storing MD5 hex digests
    of the assigned string value.
    """
    digest_size = 32
    empty = hashlib.sha3_256(b'').hexdigest()
    protected = True

    def __set__(self, instance, value):
        digest = hashlib.sha3_256(value.encode('utf-8')).hexdigest()
        super().__set__(instance, digest)

    def validate(self, value):
        if self.required and value == self.empty:
            raise ValueError('Attribute {0} is mandatory.'.format(self.name))
