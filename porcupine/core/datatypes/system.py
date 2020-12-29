from porcupine import db, exceptions
from porcupine.hinting import TYPING
from porcupine.contract import contract
from porcupine.core.context import system_override, context
from porcupine.core.services import db_connector
from porcupine.core.utils import permissions, date, add_uniques, \
    remove_uniques, get_content_class
from .collection import ItemCollection
from .common import String
from .counter import Counter
from .atomicmap import AtomicMap, AtomicMapValue
from .reference import ReferenceN


class AclValue(AtomicMapValue):
    def is_set(self) -> bool:
        return self._dct is not None

    def is_partial(self) -> bool:
        return self._dct is not None and '__partial__' in self._dct

    async def reset(self, acl, partial=False, replace=False):
        if partial:
            acl['__partial__'] = True
        # set acl
        await super().reset(acl, replace=replace)


class Acl(AtomicMap):
    def __init__(self):
        super().__init__(default=None, accepts=(int, ),
                         protected=True, allow_none=True)

    def getter(self, instance, value=None):
        return AclValue(self, instance, value)

    @staticmethod
    async def can_modify(instance):
        user_role = await permissions.resolve(instance, context.user)
        return user_role == permissions.COORDINATOR

    # HTTP views
    async def get(self, instance, _):
        return await instance.effective_acl


class ChildrenCollection(ItemCollection):
    async def add(self, *items: TYPING.ANY_ITEM_CO):
        parent = self._inst
        parent_id = parent.id
        user = context.user
        shortcut = get_content_class('Shortcut')

        await super().add(*items)

        for item in items:
            if not item.__is_new__:
                raise exceptions.DBAlreadyExists('Object already exists')

            with system_override():
                item.owner = user.id
                item.created = item.modified = date.utcnow()
                item.modified_by = user.name
                item.parent_id = parent_id
                item.p_type = parent.content_class

            expire_times = [item.expires_at, parent.expires_at]
            if isinstance(item, shortcut):
                target = await item.get_target()
                expire_times.append(target.expires_at)
            if any(expire_times):
                item.expires_at = min([t for t in expire_times if t])

            # insert item to DB
            await context.txn.insert(item)

    async def remove(self, *items: TYPING.ANY_ITEM_CO):
        await super().remove(*items)
        for item in items:
            await context.txn.delete(item)

    def is_consistent(self, item):
        return self._inst.id == item.parent_id


class Children(ReferenceN):
    name = None

    def __init__(self, **kwargs):
        super().__init__(readonly=True, **kwargs)

    def getter(self, instance, value=None):
        return ChildrenCollection(self, instance)

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
        shortcut = get_content_class('Shortcut')
        if context.user.id == 'system':
            # allow for system
            return True
        if isinstance(item, shortcut):
            target = await db_connector().get(item.target)
            if target:
                await super().accepts_item(target)
        return await super().accepts_item(item)

    # permission providers
    @staticmethod
    async def can_add(container, *items):
        return await container.can_append(context.user)

    @staticmethod
    async def can_remove(_, *items):
        return all([await item.can_delete(context.user) for item in items])

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
        super().__init__(readonly=True, protected=True, store_as='dl',
                         lock_on_update=True)

    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
        if value and not old_value:
            # recycled
            remove_uniques(instance)
        elif not value and old_value:
            # restored
            await add_uniques(instance)

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

    async def on_create(self, instance, value):
        super().on_create(instance, value)
        await add_uniques(instance)
        instance.reset_effective_acl()

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        remove_uniques(instance)
        await add_uniques(instance)
        instance.reset_effective_acl()

    def on_delete(self, instance, value):
        super().on_delete(instance, value)
        remove_uniques(instance)

    @contract(accepts=str)
    @db.transactional()
    async def put(self, instance, request):
        target_id = request.json
        if target_id != instance.parent_id:
            target = await db.get_item(request.json, quiet=False)
            await instance.move_to(target)
        return True
