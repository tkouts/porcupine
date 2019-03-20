from functools import wraps


class Observable(type):
    watch_list = []

    def __new__(mcs, name, bases, dct):
        return super().__new__(mcs, name, bases, dct)

    def __init__(cls, name, bases, dct):
        for handler in ('on_before_mutate', 'on_after_mutate'):
            if not hasattr(cls, handler):
                setattr(cls, handler, None)
        for member_name in cls.watch_list:
            member = getattr(cls, member_name)
            setattr(cls, member_name, Observable.watcher(member))
        super().__init__(name, bases, dct)

    @staticmethod
    def watcher(method):
        @wraps(method)
        def mutation_watcher(self, *args, **kwargs):
            if self.on_before_mutate is not None:
                self.on_before_mutate()
            result = method(self, *args, **kwargs)
            if self.on_after_mutate is not None:
                self.on_after_mutate()
            return result
        return mutation_watcher


class ObservableDict(dict, metaclass=Observable):
    # TODO: add mutating members
    watch_list = ['__setitem__', '__delitem__', 'setdefault']
