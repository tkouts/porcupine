import abc
from .datatype import DataType
from typing import Type

DT = Type[DataType]


class AsyncSetterValue(metaclass=abc.ABCMeta):
    __slots__ = ()

    @abc.abstractmethod
    async def reset(self, value):
        raise NotImplementedError


class AsyncSetter(DT, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def getter(self, instance, value=None):
        return NotImplementedError

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.storage == '__storage__':
            value = super().__get__(instance, owner)
        else:
            # external
            value = None
        return self.getter(instance, value)

    def __set__(self, instance, value):
        raise AttributeError('Cannot directly set the {0}. '
                             'Use the reset method instead.'.format(self.name))
