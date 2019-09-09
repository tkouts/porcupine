import collections


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
