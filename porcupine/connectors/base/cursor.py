import abc
from typing import AsyncIterable
from namedlist import namedlist
from porcupine.core.stream.streamer import IdStreamer


class Inf:
    def __eq__(self, other):
        return isinstance(other, Inf)

    def __ge__(self, other):
        return True

    def __gt__(self, other):
        return True

    def __le__(self, other):
        return isinstance(other, Inf)

    def __lt__(self, other):
        return False

    def __repr__(self):
        return 'INF'


class MinusInf:
    def __eq__(self, other):
        return isinstance(other, MinusInf)

    def __ge__(self, other):
        return isinstance(other, MinusInf)

    def __gt__(self, other):
        return False

    def __le__(self, other):
        return True

    def __lt__(self, other):
        return True

    def __repr__(self):
        return '-INF'


inf = Inf()
minus_inf = MinusInf()


class Range(namedlist('Range', 'l_bound l_inclusive u_bound, u_inclusive')):
    def comparable_bounds(self):
        start, end = self.l_bound, self.u_bound
        if start is None:
            start = minus_inf
        if end is None:
            end = inf
        return start, end

    def __contains__(self, value):
        if self.l_bound is not None:
            cmp_value = [-1]
            if self.l_inclusive:
                cmp_value.append(0)
            cmp = (self.l_bound > value) - (self.l_bound < value)
            if cmp not in cmp_value:
                return False
        if self.u_bound is not None:
            cmp_value = [1]
            if self.u_inclusive:
                cmp_value.append(0)
            # cmp is gone
            cmp = (self.u_bound > value) - (self.u_bound < value)
            if cmp not in cmp_value:
                return False
        return True

    def intersection(self, other):
        if isinstance(other, Range):
            s1, e1 = self.comparable_bounds()
            s2, e2 = other.comparable_bounds()

            if s1 <= e2 and e1 >= s2:
                if s1 == e2 and not(self.l_inclusive or other.u_inclusive):
                    return None
                if e1 == s2 and not(self.u_inclusive and other.l_inclusive):
                    return None

                if s1 == s2:
                    l_bound = s1
                    l_inclusive = self.l_inclusive and other.l_inclusive
                elif s1 > s2:
                    l_bound = s1
                    l_inclusive = self.l_inclusive
                else:
                    l_bound = s2
                    l_inclusive = other.l_inclusive

                if e1 == e2:
                    u_bound = e1
                    u_inclusive = self.u_inclusive and other.u_inclusive
                elif e1 > e2:
                    u_bound = e2
                    u_inclusive = other.u_inclusive
                else:
                    u_bound = e1
                    u_inclusive = self.u_inclusive

                if l_bound is minus_inf:
                    l_bound = None
                if u_bound is inf:
                    u_bound = None
                return Range(l_bound=l_bound, l_inclusive=l_inclusive,
                             u_bound=u_bound, u_inclusive=u_inclusive)
        elif other in self:
            return other

    def union(self, other):
        if isinstance(other, Range):
            s1, e1 = self.comparable_bounds()
            s2, e2 = other.comparable_bounds()

            if s1 <= e2 and e1 >= s2:
                if s1 == e2 and not(self.l_inclusive or other.u_inclusive):
                    return None
                if e1 == s2 and not(self.u_inclusive or other.l_inclusive):
                    return None

                # l_bound = min(s1, s2)
                if s1 == s2:
                    l_bound = s1
                    l_inclusive = self.l_inclusive or other.l_inclusive
                elif s1 > s2:
                    l_bound = s2
                    l_inclusive = other.l_inclusive
                else:
                    l_bound = s1
                    l_inclusive = self.l_inclusive

                # u_bound = max(e1, e2)
                if e1 == e2:
                    u_bound = e1
                    u_inclusive = self.u_inclusive or other.u_inclusive
                elif e1 > e2:
                    u_bound = e1
                    u_inclusive = self.u_inclusive
                else:
                    u_bound = e2
                    u_inclusive = other.u_inclusive

                if l_bound is minus_inf:
                    l_bound = None
                if u_bound is inf:
                    u_bound = None
                return Range(l_bound=l_bound, l_inclusive=l_inclusive,
                             u_bound=u_bound, u_inclusive=u_inclusive)
        elif other in self:
            return self


class BaseCursor(IdStreamer, metaclass=abc.ABCMeta):
    def __init__(self, index):
        self.index = index
        super().__init__(self.get_iterator())

    @property
    def is_ranged(self):
        return self.iterator.is_ranged

    @property
    def bounds(self):
        return self.iterator.bounds

    def set_scope(self, scope):
        self.iterator.set_scope(scope)

    def set(self, v):
        self.iterator.set(v)

    def reverse(self):
        self.iterator.reverse()

    def __repr__(self):
        return f'{self.__class__.__name__}(scope="{self.iterator.scope}", ' \
               f'bounds={self.iterator.bounds})'

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

    @property
    def bounds(self):
        return self._bounds

    @property
    def scope(self):
        return self._scope

    def set_scope(self, scope):
        self._scope = scope

    def set(self, v):
        self._bounds = v

    def reverse(self):
        self._reversed = not self._reversed

    @abc.abstractmethod
    def __aiter__(self):
        raise NotImplementedError
