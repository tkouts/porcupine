import abc
from typing import AsyncIterable
from porcupine.core.stream.streamer import Streamer


class Range:
    """
    Range objects are used for setting cursor boundaries.
    """
    def __init__(self, lower_bound=None, upper_bound=None):
        self.l_bound = lower_bound
        self.l_inclusive = False
        self.u_bound = upper_bound
        self.u_inclusive = False
        # self.set_lower_bound(lower_bound)
        # self.set_upper_bound(upper_bound)

    def set_lower_bound(self, lower_bound, inclusive=False):
        self.l_bound = lower_bound
        self.l_inclusive = inclusive

    def set_upper_bound(self, upper_bound, inclusive=False):
        self.u_bound = upper_bound
        self.u_inclusive = inclusive

    # def __contains__(self, value):
    #     if self.l_bound is not None:
    #         cmp_value = [-1]
    #         if self.l_inclusive:
    #             cmp_value.append(0)
    #         cmp = (self.l_bound > value) - (self.l_bound < value)
    #         if cmp not in cmp_value:
    #             return False
    #     if self.u_bound is not None:
    #         cmp_value = [1]
    #         if self.u_inclusive:
    #             cmp_value.append(0)
    #         cmp = (self.u_bound > value) - (self.u_bound < value)
    #         if cmp not in cmp_value:
    #             return False
    #     return True


class BaseCursor(Streamer, metaclass=abc.ABCMeta):
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

    def set_range(self, lower_bound, upper_bound):
        self.iterator.set_range(lower_bound, upper_bound)

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

    def set_range(self, lower_bound, upper_bound):
        self._bounds = Range(lower_bound, upper_bound)

    def reverse(self):
        self._reversed = not self._reversed

    @abc.abstractmethod
    def __aiter__(self):
        raise NotImplementedError
