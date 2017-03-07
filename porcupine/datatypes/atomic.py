"""
Porcupine atomic data types
===========================
"""
from porcupine import db
from porcupine.datatypes import DataType


# from porcupine.utils import date


class Atomic(DataType):
    """
    Atomic attribute data type
    """
    def __init__(self, safe_type, default=None, **kwargs):
        self.safe_type = safe_type
        default = default if default is not None else safe_type()
        super(Atomic, self).__init__(default=default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = db._db.get_atomic(instance.id, self.name)
        if value is None:
            return self.default
        else:
            return value

    # @db.requires_transactional_context
    def __set__(self, instance, value):
        if value != self.default:
            db._db.set_atomic(instance.id, self.name, value)

    def clone(self, instance, memo):
        self.__set__(instance, self.__get__(instance, None))

    def on_delete(self, instance, value, is_permanent):
        if is_permanent:
            db._db.delete_atomic(instance.id, self.name)


class AtomicTimestamp(Atomic):
    """
    Atomic timestamp
    """
    def __init__(self, **kwargs):
        super(AtomicTimestamp, self).__init__(float, default=0.0, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super(AtomicTimestamp, self).__get__(instance, owner)
        if value is not None:
            return date.Date(value)

    def __set__(self, instance, value):
        super(AtomicTimestamp, self).__set__(
            instance, value.value if isinstance(value, date.Date) else value)


class CounterValue(int):

    def __new__(cls, instance, descriptor, x=0):
        obj = super(CounterValue, cls).__new__(cls, x)
        obj.__instance = instance
        obj.__descriptor = descriptor
        return obj

    # @db.requires_transactional_context
    def incr(self, y):
        db._db.incr(self.__instance,
                    self.__descriptor.name,
                    y,
                    self.__descriptor.default)
        return self + y

    # @db.requires_transactional_context
    def decr(self, y):
        db._db.incr(self.__instance,
                    self.__descriptor.name,
                    -y,
                    self.__descriptor.default)
        return self - y


class AtomicCounter(Atomic):
    """
    Atomic counter
    """
    def __init__(self, default=0, **kwargs):
        super(AtomicCounter, self).__init__(int, default=default, **kwargs)

    def __get__(self, instance, owner) -> int:
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None:
            return CounterValue(instance, self, value)
