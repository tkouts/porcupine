from unittest.mock import sentinel
from dataclasses import make_dataclass


EMPTY = sentinel.Empty


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


class BaseBoundary:
    __slots__ = 'value'
    is_fixed = True

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f'{repr(self.value)}'

    def comparable_bounds(self):
        raise NotImplementedError

    def __contains__(self, value):
        raise NotImplementedError

    def intersection(self, other: 'BaseBoundary'):
        raise NotImplementedError

    def union(self, other: 'BaseBoundary'):
        raise NotImplementedError


class FixedBoundary(BaseBoundary):
    def comparable_bounds(self):
        return self.value, self.value

    def __contains__(self, value):
        return self.value == value

    def intersection(self, other: BaseBoundary):
        if not other.is_fixed:
            return other.intersection(self)
        elif other.value == self.value:
            return self
        return EMPTY

    def union(self, other: 'BaseBoundary'):
        if not other.is_fixed:
            return other.union(self)
        elif other.value == self.value:
            return self
        return EMPTY


Range = make_dataclass('Range',
                       ['l_bound', 'l_inclusive', 'u_bound', 'u_inclusive'])


class RangedBoundary(BaseBoundary):
    is_fixed = False

    def __init__(self, l_bound=None, l_inclusive=True,
                 u_bound=None, u_inclusive=True):
        value = Range(l_bound, l_inclusive, u_bound, u_inclusive)
        super().__init__(value)

    def comparable_bounds(self):
        start, end = self.value.l_bound, self.value.u_bound
        if start is None:
            start = minus_inf
        if end is None:
            end = inf
        return start, end

    def __contains__(self, value):
        rang = self.value
        if rang.l_bound is not None:
            cmp_value = [-1]
            if rang.l_inclusive:
                cmp_value.append(0)
            cmp = (rang.l_bound > value) - (rang.l_bound < value)
            if cmp not in cmp_value:
                return False
        if rang.u_bound is not None:
            cmp_value = [1]
            if rang.u_inclusive:
                cmp_value.append(0)
            # cmp is gone
            cmp = (rang.u_bound > value) - (rang.u_bound < value)
            if cmp not in cmp_value:
                return False
        return True

    def intersection(self, other: BaseBoundary):
        rang = self.value
        other_value = other.value
        if not other.is_fixed:
            s1, e1 = self.comparable_bounds()
            s2, e2 = other.comparable_bounds()

            if s1 <= e2 and e1 >= s2:
                if s1 == e2 and not(rang.l_inclusive or
                                    other_value.u_inclusive):
                    return None
                if e1 == s2 and not(rang.u_inclusive and
                                    other_value.l_inclusive):
                    return None

                if s1 == s2:
                    l_bound = s1
                    l_inclusive = rang.l_inclusive and other_value.l_inclusive
                elif s1 > s2:
                    l_bound = s1
                    l_inclusive = rang.l_inclusive
                else:
                    l_bound = s2
                    l_inclusive = other_value.l_inclusive

                if e1 == e2:
                    u_bound = e1
                    u_inclusive = rang.u_inclusive and other_value.u_inclusive
                elif e1 > e2:
                    u_bound = e2
                    u_inclusive = other_value.u_inclusive
                else:
                    u_bound = e1
                    u_inclusive = rang.u_inclusive

                if l_bound is minus_inf:
                    l_bound = None
                if u_bound is inf:
                    u_bound = None
                return RangedBoundary(
                    l_bound=l_bound, l_inclusive=l_inclusive,
                    u_bound=u_bound, u_inclusive=u_inclusive
                )
        elif other_value in self:
            return other
        return EMPTY

    def union(self, other: BaseBoundary):
        rang = self.value
        other_value = other.value
        if not other.is_fixed:
            s1, e1 = self.comparable_bounds()
            s2, e2 = other.comparable_bounds()

            if s1 <= e2 and e1 >= s2:
                if s1 == e2 and not(rang.l_inclusive or
                                    other_value.u_inclusive):
                    return None
                if e1 == s2 and not(rang.u_inclusive or
                                    other_value.l_inclusive):
                    return None

                # l_bound = min(s1, s2)
                if s1 == s2:
                    l_bound = s1
                    l_inclusive = rang.l_inclusive or other_value.l_inclusive
                elif s1 > s2:
                    l_bound = s2
                    l_inclusive = other_value.l_inclusive
                else:
                    l_bound = s1
                    l_inclusive = rang.l_inclusive

                # u_bound = max(e1, e2)
                if e1 == e2:
                    u_bound = e1
                    u_inclusive = rang.u_inclusive or other_value.u_inclusive
                elif e1 > e2:
                    u_bound = e1
                    u_inclusive = rang.u_inclusive
                else:
                    u_bound = e2
                    u_inclusive = other_value.u_inclusive

                if l_bound is minus_inf:
                    l_bound = None
                if u_bound is inf:
                    u_bound = None
                return RangedBoundary(
                    l_bound=l_bound, l_inclusive=l_inclusive,
                    u_bound=u_bound, u_inclusive=u_inclusive
                )
        elif other_value in self:
            return self
        return EMPTY
