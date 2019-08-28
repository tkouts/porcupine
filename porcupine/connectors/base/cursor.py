import abc
from typing import AsyncIterable
from namedlist import namedlist
from porcupine.core.stream.streamer import IdStreamer


Range = namedlist('Range', 'l_bound l_inclusive u_bound, u_inclusive')


class BaseCursor(IdStreamer, metaclass=abc.ABCMeta):
    def __init__(self, index):
        self.index = index
        super().__init__(self.get_iterator())

    @property
    def is_ranged(self):
        return self.iterator.is_ranged

    def set_scope(self, scope):
        self.iterator.set_scope(scope)

    def set(self, v):
        self.iterator.set(v)

    def reverse(self):
        self.iterator.reverse()

    @abc.abstractmethod
    def get_iterator(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class AbstractCursorIterator(AsyncIterable, metaclass=abc.ABCMeta):
    def __init__(self, index):
        self.index = index
        self._bounds = None
        self._reversed = False
        self._scope = None

    @property
    def is_ranged(self):
        return isinstance(self._bounds, Range)

    def set_scope(self, scope):
        self._scope = scope

    def set(self, v):
        self._bounds = v

    def reverse(self):
        self._reversed = not self._reversed

    @abc.abstractmethod
    def __aiter__(self):
        raise NotImplementedError
