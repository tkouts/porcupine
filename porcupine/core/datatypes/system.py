from porcupine import context, exceptions
from porcupine.utils import permissions
from .mutable import Dictionary
from .common import String
from .reference import ReferenceN, ItemCollection


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
    protected = True


class Children(ReferenceN):
    readonly = True

    def __get__(self, instance, owner):
        if instance is None:
            # create a separate instance per owner
            # with accepting the container's allowed types
            if self.name not in owner.__dict__:
                children = Children(default=self._default,
                                    accepts=owner.containment)
                setattr(owner, 'children', children)
                return children
            return self
        return super().__get__(instance, owner)

    async def get(self, request, instance, resolve=False):
        return await super().get(request, instance, True)
