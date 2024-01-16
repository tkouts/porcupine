import collections
from typing import Optional


class FrozenDict(collections.Mapping):
    def __init__(self, dct: dict):
        self._dct = dct

    def __getitem__(self, item):
        return self._dct[item]

    def __iter__(self):
        return iter(self._dct)

    def __len__(self):
        return len(self._dct)

    def to_json(self):
        return {**self._dct}


class OptionalFrozenDict(FrozenDict):
    def __init__(self, dct: Optional[dict]):
        if dct is not None:
            super().__init__(dct)
        else:
            self._dct = None

    def __getitem__(self, item):
        if self._dct is None:
            raise KeyError(item)
        return super().__getitem__(item)

    def __iter__(self):
        if self._dct is None:
            raise StopIteration
        return super().__iter__()

    def __len__(self):
        if self._dct is None:
            return 0
        return super().__len__()

    def to_json(self):
        if self._dct is None:
            return None
        return super().to_json()


class WriteOnceDict(collections.MutableMapping, dict):
    # dict implementations to override the MutableMapping versions
    __getitem__ = dict.__getitem__
    __iter__ = dict.__iter__
    __len__ = dict.__len__

    def __delitem__(self, key):
        raise KeyError('Read-only dictionary')

    def __setitem__(self, key, value):
        if key in self:
            raise KeyError('{0} has already been set'.format(key))
        dict.__setitem__(self, key, value)


def identity(value):
    """Identity function."""
    return value
