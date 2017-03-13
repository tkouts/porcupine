from porcupine import context, exceptions
from porcupine.utils import permissions
from .mutable import Dictionary
from .common import String
from .collection import ItemCollection, Collection


class Acl(Dictionary):
    async def on_change(self, instance, value, old_value):
        acl = await instance.applied_acl
        user_role = await permissions.resolve(acl, context.user)
        if user_role < permissions.COORDINATOR:
            raise exceptions.Forbidden(
                'The user does not have permissions '
                'to modify the access control list.')
        super().on_change(instance, value, old_value)


class SchemaSignature(String):
    readonly = True


class Children(ItemCollection):
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return Collection(self, instance, instance.containment)
