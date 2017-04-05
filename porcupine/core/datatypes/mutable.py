import copy
from collections import MutableSequence, MutableMapping
from porcupine.utils.observables import ObservableList, ObservableDict
from .datatype import DataType


class OList(ObservableList):
    def __init__(self, descriptor, instance, seq=()):
        self._descriptor = descriptor
        self._instance = instance
        self._prev = None
        super().__init__(seq)

    def on_before_mutate(self):
        self._prev = copy.deepcopy(self[:])

    def on_after_mutate(self):
        self._descriptor.snapshot(self._instance, self, self._prev)


class List(DataType):
    """List data type"""
    safe_type = list

    def __init__(self, default=None, **kwargs):
        if default is None and not kwargs.get('allow_none') \
                and not self.allow_none:
            default = []
        super().__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if self.readonly:
            return tuple(value)
        if value is not None:
            return OList(self, instance, value)

    def set_default(self, instance, value=None):
        if value is None:
            value = self._default
        if isinstance(value, MutableSequence):
            value = copy.deepcopy(value)
        super().set_default(instance, value)


class ODict(ObservableDict):
    def __init__(self, descriptor, instance, seq=(), **kwargs):
        self._descriptor = descriptor
        self._instance = instance
        self._prev = None
        super().__init__(seq, **kwargs)

    def on_before_mutate(self):
        self._prev = copy.deepcopy(dict(self.items()))

    def on_after_mutate(self):
        self._descriptor.snapshot(self._instance, self, self._prev)


class Dictionary(DataType):
    """Dictionary data type"""
    safe_type = dict

    def __init__(self, default=None, **kwargs):
        if default is None and not kwargs.get('allow_none') \
                and not self.allow_none:
            default = {}
        super().__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None:
            return ODict(self, instance, value)

    def set_default(self, instance, value=None):
        if value is None:
            value = self._default
        if isinstance(value, MutableMapping):
            value = copy.deepcopy(value)
        super().set_default(instance, value)
