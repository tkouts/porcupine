import collections
from porcupine.hinting import TYPING
from sortedcontainers import SortedList, SortedKeyList


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


class AsyncList(collections.AsyncIterable, TYPING.SORTED_LIST_TYPE):
    def __new__(cls, iterable=None, key=None, async_reverse=False):
        return super().__new__(cls, iterable, key)

    def __init__(self, async_reverse):
        self.async_reverse = async_reverse

    @staticmethod
    def reduce_sort(sorted_list, chunk: list):
        sorted_list.update(chunk)
        return sorted_list

    async def __aiter__(self):
        it = self
        if self.async_reverse:
            it = reversed(it)
        for i in it:
            yield i


class AsyncReversedList(AsyncList, list):
    def __init__(self, iterable=None):
        super().__init__(async_reverse=True)
        list.__init__(self, iterable or [])

    @staticmethod
    def populate(l, chunk):
        l.extend(chunk)
        return l


# noinspection PyAbstractClass
class AsyncSortedList(AsyncList, SortedList):
    def __init__(self, iterable=None, async_reverse=False):
        super().__init__(async_reverse)
        SortedList.__init__(self, iterable)


# noinspection PyAbstractClass
class AsyncSortedKeyList(AsyncList, SortedKeyList):
    def __init__(self, iterable=None, key=identity, async_reverse=False):
        super().__init__(async_reverse)
        SortedKeyList.__init__(self, iterable, key)
