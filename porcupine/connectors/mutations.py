import abc
from enum import Enum


class Formats(Enum):
    JSON = 0
    STRING = 1
    BINARY = 2

    @staticmethod
    def guess_format(v):
        if isinstance(v, str):
            return Formats.STRING
        elif isinstance(v, (bytes, bytearray)):
            return Formats.BINARY
        return Formats.JSON


class SubDocument(Enum):
    UPSERT = 0
    COUNTER = 1
    INSERT = 2
    REMOVE = 4


class DBMutation(metaclass=abc.ABCMeta):
    __slots__ = 'key', 'value', 'ttl', 'fmt'

    def __init__(self, key, value, ttl, fmt):
        self.key = key
        self.value = value
        self.ttl = ttl or None
        self.fmt = fmt

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'key="{self.key}" '
            f'value={self.value} '
            f'ttl={self.ttl} '
            f'fmt={self.fmt})'
        )

    @abc.abstractmethod
    def apply(self, connector):
        raise NotImplementedError


class Insertion(DBMutation):
    def apply(self, connector):
        return connector.insert_raw(self.key, self.value, self.ttl, self.fmt)


class Upsertion(DBMutation):
    def apply(self, connector):
        return connector.upsert_raw(self.key, self.value, self.ttl, self.fmt)


class Deletion(DBMutation):
    def __init__(self, key):
        super().__init__(key, None, None, None)

    def __repr__(self):
        return f'{self.__class__.__name__}(key="{self.key}")'

    def apply(self, connector):
        return connector.delete(self.key)


class SubDocumentMutation(DBMutation):
    def __init__(self, key, value):
        super().__init__(key, value, None, None)

    def __repr__(self):
        return f'{self.__class__.__name__}(key="{self.key}" value={self.value})'

    def apply(self, connector):
        return connector.mutate_in(self.key, self.value)


class Append(DBMutation):
    def apply(self, connector):
        return connector.append_raw(self.key, self.value, self.ttl, self.fmt)
