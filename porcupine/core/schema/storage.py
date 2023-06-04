from unittest.mock import sentinel
from porcupine.hinting import TYPING
from typing import Dict, Any, Optional

UNSET = sentinel.NotSet


class Record:
    __slots__ = ()
    fields = frozenset()

    def __init__(self, store: Optional[Dict[str, Any]] = None):
        if store is not None:
            fields = self.fields
            for field, value in store.items():
                if field in fields:
                    setattr(self, field, value)

    def __getitem__(self, index):
        return getattr(self, self.__slots__[index], UNSET)

    def __getattr__(self, field):
        if field in self.fields:
            return UNSET
        raise AttributeError(
            f'{type(self).__name__} has no field named "{field}"')

    def __len__(self):
        return len(self.__slots__)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return all(item == iota for item, iota in zip(self, other))

    def __repr__(self):
        fields = [f'{field}={repr(item)}'
                  for field, item in zip(self.__slots__, self)]
        return '%s(%s)' % (type(self).__name__, ', '.join(fields))

    def __getstate__(self):
        return tuple(self.as_dict().items())

    def __setstate__(self, state):
        self.__init__(dict(state))

    update = __init__

    def as_dict(self) -> dict:
        # do not persist UNSET as it is the default value
        return {k: v for k, v in zip(self.__slots__, self)
                if v is not UNSET}


def storage(typename, field_names) -> TYPING.STORAGE_TYPE:
    record_type = type(
        typename, (Record, ),
        {
            '__slots__': field_names,
            'fields': frozenset(field_names)
        }
    )
    return record_type
