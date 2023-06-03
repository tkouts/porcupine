import abc
import weakref

from porcupine import exceptions
from porcupine.core.context import context
from porcupine.core.datatypes.datatype import DataType
from porcupine.hinting import TYPING


class AsyncSetterValue:
    def __init__(self,
                 descriptor: TYPING.DT_CO,
                 instance: TYPING.ANY_ITEM_CO):
        self._desc = descriptor
        self._inst = weakref.ref(instance)

    async def reset(self, value):
        descriptor, instance = self._desc, self._inst()
        if not await descriptor.can_modify(instance):
            raise exceptions.Forbidden('Forbidden')
        context.txn.reset_mutations(instance, f'{descriptor.storage_key}.')
        super(AsyncSetter, descriptor).__set__(instance, value)


class AsyncSetter(DataType, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def getter(self, instance, value=None):
        raise NotImplementedError

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
        raise AttributeError(
            f'Cannot directly set the {self.name}. '
            'Use the reset method instead.'
        )

    @staticmethod
    async def can_modify(instance):
        return await instance.can_update(context.user)
