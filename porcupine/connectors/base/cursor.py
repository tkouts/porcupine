import abc


class Range:
    """
    Range objects are used for setting cursor boundaries.
    The bounds are tuples of two elements. The first element contains the
    value while the second is a boolean indicating if the value is
    inclusive.
    """
    def __init__(self, lower_bound=None, upper_bound=None):
        self._lower_value = None
        self._lower_inclusive = None
        self._upper_value = None
        self._upper_inclusive = None
        self.set_lower_bound(lower_bound)
        self.set_upper_bound(upper_bound)

    def set_lower_bound(self, lower_bound, inclusive=False):
        if lower_bound is not None:
            self._lower_value = lower_bound
            self._lower_inclusive = inclusive
        else:
            self._lower_value = None
            self._lower_inclusive = False

    def set_upper_bound(self, upper_bound, inclusive=False):
        if upper_bound is not None:
            self._upper_value = upper_bound
            self._upper_inclusive = inclusive
        else:
            self._upper_value = None
            self._upper_inclusive = False

    def __contains__(self, value):
        if self._lower_value is not None:
            cmp_value = [-1]
            if self._lower_inclusive:
                cmp_value.append(0)
            cmp = (self._lower_value > value) - \
                  (self._lower_value < value)
            if cmp not in cmp_value:
                return False
        if self._upper_value is not None:
            cmp_value = [1]
            if self._upper_inclusive:
                cmp_value.append(0)
            cmp = (self._upper_value > value) - \
                  (self._upper_value < value)
            if cmp not in cmp_value:
                return False
        return True


class AbstractCursor(metaclass=abc.ABCMeta):
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

    @property
    @abc.abstractmethod
    def size(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __aiter__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
