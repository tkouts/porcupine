from functools import partial
from porcupine.core.oql.feeder import Feeder


class FeederProxy:
    __slots__ = '_feeder_type', '_args', '_kwargs'

    def __init__(self, feeder_type, *args, **kwargs):
        self._feeder_type = feeder_type
        self._args = args
        self._kwargs = kwargs

    @staticmethod
    def factory(feeder_type, *args, **kwargs) -> partial:
        return partial(FeederProxy(feeder_type, *args, **kwargs))

    @property
    def default_priority(self):
        return self._feeder_type.default_priority

    def is_of_type(self, feeder_type):
        return self._feeder_type is feeder_type

    def is_ordered_by(self, field_list):
        return self().is_ordered_by(field_list)

    def __call__(self, **kwargs) -> Feeder:
        # unpack args
        args = [
            a(**kwargs) if isinstance(a, partial) else a
            for a in self._args
        ]
        # merge kwargs
        kwargs = {**self._kwargs, **kwargs}
        # print(args, kwargs)
        return self._feeder_type(*args, **kwargs)
