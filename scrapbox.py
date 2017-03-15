from functools import wraps


class MutableWatcher(type):
    mutating_members = []

    def __init__(cls, name, bases, dct):
        print('INITI')
        for member_name in cls.mutating_members:
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


class Dict(list, metaclass=MutableWatcher):
    mutating_members = ['__setitem__']

    def __init__(self, instance, seq=()):
        self._instance = instance
        super().__init__(seq)

    # def __getattribute__(self, item):
    #     print(item)
    #     return getattr(super(), item)

    # def __setitem__(self, key, value):
    #     print('setting', key)
    #     super().__setitem__(key, value)


a = Dict(None, [1])

# b = Dict(None, 2)

# Dict.fromkeys(['1', '2'])

# a.update({'sdfsdf': 1})
a[0] = 1
# a.setdefault('sdfsdf', 1)

print(a._instance)
