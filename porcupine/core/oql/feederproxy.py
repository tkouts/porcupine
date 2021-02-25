from porcupine.core.oql.feeder import Feeder


class Argument:
    __slots__ = '_value'

    def __init__(self, value):
        self._value = value

    def value(self, _options):
        return self._value


class FeederProxy(Argument):
    __slots__ = ('_feeder_type', '_kwargs')

    def __init__(self, feeder_type, **kwargs):
        super().__init__(None)
        self._feeder_type = feeder_type
        # wrap kwargs
        self._kwargs = {
            k: v if type(v) is FeederProxy else Argument(v)
            for k, v in kwargs.items()
        }

    @property
    def priority(self):
        return self._feeder_type.priority

    def set_argument(self, name: str, v):
        self._kwargs[name] = v if type(v) is FeederProxy else Argument(v)

    def value(self, options) -> Feeder:
        kwargs = {
            k: v.value(options)
            for k, v in self._kwargs.items()
        }
        return self._feeder_type(**kwargs, options=options)
