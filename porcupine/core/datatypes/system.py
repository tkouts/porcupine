from porcupine import db, exceptions
from porcupine.response import json
from porcupine.hinting import TYPING
from porcupine.contract import contract
from porcupine.core.context import system_override, context
from porcupine.core.services import db_connector
from porcupine.core.utils import date
from porcupine.core.schemaregistry import get_content_class
from porcupine.core.accesscontroller import Roles
# from porcupine.core.stream.streamer import ItemStreamer
# from .collection import ItemCollection
# from .common import String
from .counter import Counter
from .atomicmap import AtomicMap, AtomicMapValue
# from .reference import ReferenceN
from .relator import RelatorN, Relator1  # , RelatorCollection
from .collection import ItemCollection
# from pypika.terms import Parameter
# from functools import cached_property


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
        user_role = await Roles.resolve(instance, context.user)
        return user_role == Roles.COORDINATOR

    # HTTP views
    def get(self, instance, _):
        return instance.effective_acl


class ChildrenCollection(ItemCollection):
    async def add(self, *items: TYPING.ANY_ITEM_CO):
        parent = self._inst()
        # parent_id = parent.id
        user = context.user
        shortcut = get_content_class('Shortcut')

        for item in items:
            if not item.__is_new__:
                raise exceptions.DBAlreadyExists('Object already exists')

            with system_override():
                item.owner = user.id
                item.created = item.modified = date.utcnow()
                item.modified_by = user.name
                item.p_type = parent.content_class

            expire_times = [item.expires_at, parent.expires_at]
            if isinstance(item, shortcut):
                target = await item.get_target()
                expire_times.append(target.expires_at)
            if any(expire_times):
                item.expires_at = min([t for t in expire_times if t])

            # insert item to DB
            await context.txn.insert(item)

        await super().add(*items)

    async def remove(self, *items: TYPING.ANY_ITEM_CO):
        await super().remove(*items)
        for item in items:
            await context.txn.delete(item)

    def is_consistent(self, item):
        return self._inst().id == item.parent_id


class Children(RelatorN):
    name = 'children'

    def __init__(self, **kwargs):
        super().__init__(
            readonly=True,
            rel_attr='parent_id',
            **kwargs
        )

    def getter(self, instance, value=None):
        return ChildrenCollection(self, instance)

    def __get__(self, instance, owner):
        if instance is None:
            if self.name not in owner.__dict__:
                # create a separate instance per owner
                # accepting the container's containment types
                clazz = type(self)
                children = clazz(accepts=owner.containment)
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

    @db.transactional()
    async def post(self, instance, request):
        if 'source' in request.args:
            # copy operation
            source = await db.get_item(request.args.get('source'), quiet=False)
            # TODO: handle items with no copy capability
            new_item = await source.copy_to(instance)
        else:
            # new item
            try:
                new_item = await instance.new_from_dict(request.json)
                await new_item.append_to(instance)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))

        location = f'/resources/{new_item.id}'

        return json(
            new_item,
            status=201,
            headers={
                'Location': location
            })

    # disallow direct assignment
    put = None


class Deleted(Counter):

    def __init__(self):
        super().__init__(readonly=True, protected=True)

    @contract(accepts=bool)
    @db.transactional()
    async def put(self, instance, request):
        if request.json:
            with system_override():
                recycle_bin = await db.get_item('RB')
            await instance.recycle_to(recycle_bin)
            return True
        return False


class ParentId(Relator1):
    def __init__(self):
        super().__init__(default=None,
                         rel_attr='children',
                         accepts=('Container', ),
                         readonly=True)

    async def on_create(self, instance, value):
        await super().on_create(instance, value)
        instance.reset_effective_acl()

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        if instance.is_collection:
            context.access_map[instance.id] = instance.access_record
        instance.reset_effective_acl()

    @contract(accepts=str)
    @db.transactional()
    async def put(self, instance, request):
        target_id = request.json
        if target_id != instance.parent_id:
            target = await db.get_item(request.json, quiet=False)
            await instance.move_to(target)
        return True
