import porcupine.core.utils.customcollections
from porcupine import context, exceptions, db, gather
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core.services.schema import SchemaMaintenance
from porcupine.core.utils import system, permissions
from .common import String
from .counter import Counter
from .mutable import Dictionary
from .reference import ReferenceN

Shortcut = None


class AclValue(porcupine.core.utils.customcollections.FrozenDict):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor: Dictionary, instance, acl):
        super().__init__(acl)
        self._desc = descriptor
        self._inst = instance

    async def reset(self, acl):
        # check user permissions
        user_role = await permissions.resolve(self._inst, context.user)
        if user_role < permissions.COORDINATOR:
            raise exceptions.Forbidden(
                'The user does not have permissions '
                'to modify the access control list.')
        super(type(self._desc), self._desc).__set__(self._inst, acl)


class Acl(Dictionary):
    protected = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.default = None

    def __get__(self, instance, owner) -> [AclValue, 'Acl']:
        if instance is None:
            return self
        dct = super().__get__(instance, owner) or {}
        return AclValue(self, instance, dct)

    def __set__(self, instance, value):
        raise AttributeError('Cannot directly set the ACL. '
                             'Use the reset method instead.')

    async def apply_acl(self, container, acl, old_acl):
        tasks = []
        children = await container.get_children()
        for child in children:
            if child.acl == old_acl:
                super().on_change(child, acl, old_acl)
                if child.is_collection:
                    tasks.append(self.apply_acl(child, acl, old_acl))
                    # await self.apply_acl(child, acl, old_acl)
        if tasks:
            await gather(*tasks)

    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
        if instance.is_collection:
            with system_override():
                await self.apply_acl(instance, value, old_value)

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, instance, request):
        acl = request.json
        await self.__get__(instance, None).reset(acl)
        await instance.update()
        return True


class SchemaSignature(String):
    required = True
    readonly = True
    protected = True

    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
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
                children = clazz(default=self.default,
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

    # disallow direct additions / assignment
    post = None
    put = None


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


class Deleted(Counter):
    readonly = True
    protected = True

    @contract(accepts=bool)
    @db.transactional()
    async def put(self, instance, request):
        if request.json:
            with system_override():
                recycle_bin = await db.get_item('RB')
            await instance.recycle_to(recycle_bin)
            return True
        return False


class ParentId(String):
    readonly = True
    allow_none = True

    @contract(accepts=str)
    @db.transactional()
    async def put(self, instance, request):
        target_id = request.json
        if target_id != instance.parent_id:
            target = await db.get_item(request.json, quiet=False)
            await instance.move_to(target)
        return True
