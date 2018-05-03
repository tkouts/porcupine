import abc
from typing import Type

from porcupine import permissions, context, exceptions
from porcupine.hinting import TYPING


class AsyncSetterValue:
    def __init__(self,
                 descriptor: TYPING.DT_CO,
                 instance: TYPING.ANY_ITEM_CO):
        self._desc = descriptor
        self._inst = instance

    async def reset(self, value):
        descriptor, instance = self._desc, self._inst
        if descriptor.write_permission > permissions.AUTHOR:
            await descriptor.check_permissions(instance)
        context.txn.reset_mutations(instance,
                                    '{0}.'.format(descriptor.storage_key))
        super(AsyncSetter, descriptor).__set__(instance, value)


class AsyncSetter(Type[TYPING.DT_CO], metaclass=abc.ABCMeta):
    write_permission = permissions.AUTHOR

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
        raise AttributeError('Cannot directly set the {0}. '
                             'Use the reset method instead.'.format(self.name))

    async def check_permissions(self, instance):
        user_role = await permissions.resolve(instance, context.user)
        if user_role < self.write_permission:
            raise exceptions.Forbidden('Forbidden')

    async def touch(self, instance):
        if self.write_permission > permissions.AUTHOR:
            await self.check_permissions(instance)
        is_set = self.storage_key in instance.__snapshot__
        if not instance.__is_new__ and not is_set:
            await instance.touch()
