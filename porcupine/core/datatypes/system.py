from porcupine import context, exceptions, db
from porcupine.utils import permissions, system
from porcupine.core.services.schema import SchemaMaintenance
from .mutable import Dictionary
from .common import String, Boolean
from .reference import ReferenceN

Shortcut = None


class Acl(Dictionary):
    allow_none = True

    async def on_change(self, instance, value, old_value):
        user_role = await permissions.resolve(instance, context.user)
        if user_role < permissions.COORDINATOR:
            raise exceptions.Forbidden(
                'The user does not have permissions '
                'to modify the access control list.')
        super().on_change(instance, value, old_value)


class SchemaSignature(String):
    required = True
    readonly = True
    protected = True

    async def on_change(self, instance, value, old_value):
        await SchemaMaintenance.clean_schema(instance.id)


class Children(ReferenceN):
    readonly = True
    name = None

    def __get__(self, instance, owner):
        if instance is None:
            if self.name not in owner.__dict__:
                # create a separate instance per owner
                # accepting the container's containment types
                clazz = type(self)
                children = clazz(default=self._default,
                                 accepts=owner.containment)
                setattr(owner, clazz.name, children)
                return children
            return self
        return super().__get__(instance, owner)

    async def accepts_item(self, item):
        global Shortcut
        if Shortcut is None:
            from porcupine.schema import Shortcut
        if context.user.id == 'system':
            # allow for system
            return True
        if isinstance(item, Shortcut):
            target = await db.connector.get(item.target)
            if target:
                await super().accepts_item(target)
        return await super().accepts_item(item)

    # HTTP views

    async def get(self, instance, request, expand=True):
        return await super().get(instance, request, True)

    # disallow direct additions
    post = None


class Items(Children):
    name = 'items'

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            resolved = super().allowed_types
            self.accepts = tuple([x for x in resolved if not x.is_collection])
        return self.accepts


class Containers(Children):
    name = 'containers'

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            resolved = super().allowed_types
            self.accepts = tuple([x for x in resolved if x.is_collection])
        return self.accepts


class Deleted(Boolean):
    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
        if not instance.__is_new__:
            uniques = [dt for dt in instance.__schema__.values()
                       if dt.unique]
            unique_keys = []
            for dt in uniques:
                storage = getattr(instance, dt.storage)
                unique_keys.append(system.get_key_of_unique(
                    instance.__storage__.pid,
                    dt.name,
                    getattr(storage, dt.storage_key)
                ))
            # print(unique_keys)
            if value:
                # soft deletion
                for unique_key in unique_keys:
                    context.txn.delete_external(unique_key)
            else:
                # item restoration
                for unique_key in unique_keys:
                    context.txn.insert_external(unique_key,
                                                instance.__storage__.id)
