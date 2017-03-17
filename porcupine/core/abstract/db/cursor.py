import abc
from porcupine.utils import permissions
from porcupine.schema import Shortcut


class Range(object):
    """
    Range objects are used for setting cursor boundaries.
    The bounds are tuples of two elements. The first element contains the
    value while the second is a boolean indicating if the value is
    inclusive.
    """

    def __init__(self, lower_bound=None, upper_bound=None):
        # self.use_packed = use_packed
        self._lower_value = None
        self._lower_inclusive = None
        self._upper_value = None
        self._upper_inclusive = None
        self.set_lower_bound(lower_bound)
        self.set_upper_bound(upper_bound)

    def set_lower_bound(self, lower_bound):
        if lower_bound is not None:
            value, inclusive = lower_bound
            # if self.use_packed:
            #     self._lower_value = pack_value(value)
            # else:
            self._lower_value = value
            # value.value if isinstance(value, Date) else value
            self._lower_inclusive = inclusive
        else:
            self._lower_value = None
            self._lower_inclusive = False

    def set_upper_bound(self, upper_bound):
        if upper_bound is not None:
            value, inclusive = upper_bound
            # if self.use_packed:
            #     self._upper_value = pack_value(value)
            # else:
            self._upper_value = value
            # value.value if isinstance(value, Date) else value
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


class AbstractCursor(object, metaclass=abc.ABCMeta):
    def __init__(self,
                 connector,
                 index,
                 fetch_mode=1,
                 enforce_permissions=True,
                 resolve_shortcuts=False):
        self.connector = connector
        self.index = index

        self.fetch_mode = fetch_mode
        self.enforce_permissions = enforce_permissions
        self.resolve_shortcuts = resolve_shortcuts

        self._value = None
        self._range = None
        self._reversed = False
        self._scope = None

    def resolve(self, v):
        item = self.connector.persist.loads(v)
        if not self.enforce_permissions:
            if self.resolve_shortcuts:
                while item is not None and isinstance(item, Shortcut):
                    item = self.connector.get(item.target)
        else:
            # check read permissions
            access = permissions.resolve(item, context.user)
            if item._is_deleted or access == permissions.NO_ACCESS:
                item = None
            elif self.resolve_shortcuts and isinstance(item, Shortcut):
                item = item.get_target()
        return item

    def eval(self, item):
        if hasattr(item, self.name):
            attr = getattr(item, self.name)
            # if self.use_packed:
            #     attr = pack_value(attr)
            if self._value is not None:
                return self._value == attr
            elif self._range is not None:
                return attr in self._range
            return True
        return False

    def set_scope(self, scope):
        self._scope = scope

    def set(self, v):
        # if self.use_packed:
        #     self._value = pack_value(v)
        # else:
        #     self._value = v.value if isinstance(v, Date) else v
        self._value = v
        self._range = None

    def set_range(self, lower_bound, upper_bound):
        self._range = Range(lower_bound, upper_bound)
        self._value = None

    def reverse(self):
        self._reversed = not self._reversed

    @abc.abstractproperty
    def size(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
