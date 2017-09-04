from porcupine import context, exceptions, db
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core.services.schema import SchemaMaintenance
from porcupine.core.utils import permissions, collections
from .common import String
from .counter import Counter
from .mutable import Dictionary
from .reference import ReferenceN
from .asyncsetter import AsyncSetter, AsyncSetterValue

Shortcut = None


class AclValue(AsyncSetterValue, collections.FrozenDict):
    __slots__ = ('_desc', '_inst')

    def __init__(self, descriptor: Dictionary, instance, acl):
        super().__init__(acl)
        self._desc = descriptor
        self._inst = instance

    def is_set(self) -> bool:
        return self._dct is not None

    def is_partial(self) -> bool:
        return self._dct is not None and '__partial__' in self._dct

    def to_dict(self):
        if self._dct is None:
            return None
        return super().to_dict()

    toDict = to_dict

    async def reset(self, acl, partial=False):
        # check user permissions
        if partial:
            acl['__partial__'] = True
        instance = self._inst
        user_role = await permissions.resolve(instance, context.user)
        if user_role < permissions.COORDINATOR:
            raise exceptions.Forbidden('Forbidden')
        # set acl
        super(Dictionary, self._desc).__set__(instance, acl)


class Acl(AsyncSetter, Dictionary):
    def __init__(self):
        super().__init__(default=None, protected=True, allow_none=True)

    def getter(self, instance, value=None):
        return AclValue(self, instance, value)

    # HTTP views
    async def get(self, instance, _):
        return await instance.effective_acl


class SchemaSignature(String):
    def __init__(self):
        super().__init__(required=True, readonly=True, protected=True)

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        await SchemaMaintenance.clean_schema(instance.id)


class Children(ReferenceN):
    name = None

    def __init__(self, **kwargs):
        super().__init__(readonly=True, **kwargs)

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

    def __init__(self):
        super().__init__(readonly=True, protected=True, store_as='dl')

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

    def __init__(self):
        super().__init__(default=None, readonly=True, allow_none=True,
                         store_as='pid')

    @contract(accepts=str)
    @db.transactional()
    async def put(self, instance, request):
        target_id = request.json
        if target_id != instance.parent_id:
            target = await db.get_item(request.json, quiet=False)
            await instance.move_to(target)
        return True
