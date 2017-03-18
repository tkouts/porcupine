from functools import wraps
from .datatype import DataType


class MutableWatcher(type):
    watch_list = []

    def __init__(cls, name, bases, dct):
        for member_name in cls.watch_list:
            member = getattr(cls, member_name)
            setattr(cls, member_name, MutableWatcher.snapshot(member))
        super().__init__(name, bases, dct)

    @staticmethod
    def snapshot(method):
        @wraps(method)
        def mutation_watcher(self, *args, **kwargs):
            print('method call', *args)
            return method(self, *args, **kwargs)
        return mutation_watcher


class GuardedList(list, metaclass=MutableWatcher):
    watch_list = ['__setitem__']

    def __init__(self, descriptor, instance, seq=()):
        self._descriptor = descriptor
        self._instance = instance
        super().__init__(seq)


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
            return GuardedList(self, instance, value)


class GuardedDict(dict, metaclass=MutableWatcher):
    watch_list = ['__setitem__']

    def __init__(self, descriptor, instance, seq=None, **kwargs):
        self._descriptor = descriptor
        self._instance = instance
        super().__init__(seq, **kwargs)


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
            return GuardedDict(self, instance, value)
