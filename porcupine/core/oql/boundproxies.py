from porcupine.core.utils.date import DateTime, Date
from porcupine.connectors.base.bounds import (
    FixedBoundary,
    RangedBoundary
)


class BoundProxyBase:
    @staticmethod
    def date_to_utc(date: Date):
        if isinstance(date, DateTime):
            date = date.in_timezone('UTC')
        return date.isoformat()

    def __call__(self, statement, v):
        raise NotImplementedError

    def intersection(self, other: 'BoundProxyBase'):
        return IntersectionBoundaryProxy(self, other)


class FixedBoundaryProxy(BoundProxyBase):
    __slots__ = 'func'

    def __init__(self, func):
        self.func = func

    def __call__(self, statement, v):
        bound = self.func(None, statement, v)
        if isinstance(bound, Date):
            bound = self.date_to_utc(bound)
        return FixedBoundary(bound)


class RangedBoundaryProxy(BoundProxyBase):
    __slots__ = 'l_bound', 'l_inclusive', 'u_bound', 'u_inclusive'

    def __init__(self,
                 l_bound=None,
                 l_inclusive=False,
                 u_bound=None,
                 u_inclusive=False):
        self.l_bound = l_bound
        self.l_inclusive = l_inclusive
        self.u_bound = u_bound
        self.u_inclusive = u_inclusive

    def __call__(self, statement, v):
        l_bound = self.l_bound and self.l_bound(None, statement, v)
        if l_bound is not None and isinstance(l_bound, Date):
            l_bound = self.date_to_utc(l_bound)
        u_bound = self.u_bound and self.u_bound(None, statement, v)
        if u_bound is not None and isinstance(u_bound, Date):
            u_bound = self.date_to_utc(u_bound)
        return RangedBoundary(
            l_bound, self.l_inclusive, u_bound, self.u_inclusive
        )


class IntersectionBoundaryProxy(BoundProxyBase):
    def __init__(self, first: BoundProxyBase, second: BoundProxyBase):
        self.first = first
        self.second = second

    def __call__(self, statement, v):
        first = self.first(statement, v)
        second = self.second(statement, v)
        return first.intersection(second)
