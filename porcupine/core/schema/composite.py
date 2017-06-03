from porcupine import db, context, exceptions
from porcupine.contract import contract
from porcupine.datatypes import String
from porcupine.core.context import system_override
from .elastic import Elastic


class Composite(Elastic):
    """
    Objects within Objects...

    Think of this as an embedded item. This class is useful
    for implementing compositions. Instances of this class
    are embedded into other items.
    Note that instances of this class have no
    security descriptor since they are embedded into other items.
    The L{security} property of such instances is actually a proxy to
    the security attribute of the object that embeds this object.
    Moreover, they do not have parent containers the way
    instances of L{GenericItem} have.

    @see: L{porcupine.datatypes.Composition}
    """
    is_composite = True
    path = String(required=True, readonly=True, protected=True,
                  immutable=True)  # type: str

    def __init__(self, dict_storage=None):
        super().__init__(dict_storage)
        if self.__is_new__ and not context.system_override:
            raise TypeError('Composite objects cannot be instantiated')

    @property
    def item_id(self):
        return self.path.split('.')[0]

    @property
    def parent_id(self):
        return self.path.split('.')[-2]

    @property
    async def is_stale(self):
        _, exists = await db.connector.exists(self.parent_id)
        return not exists

    @property
    async def item(self):
        return await db.connector.get(self.item_id)

    @property
    async def parent(self):
        return await db.connector.get(self.parent_id)

    @property
    async def is_deleted(self):
        parent = await self.item
        if parent is None:
            # stale composite
            return 1
        return parent.is_deleted

    @property
    async def acl(self):
        parent = await self.item
        return parent.acl

    async def clone(self, memo: dict=None) -> 'Composite':
        clone: 'Composite' = await super().clone(memo)
        with system_override():
            id_map = memo['_id_map_']
            clone.path = '.'.join([id_map.get(oid, oid)
                                   for oid in self.path.split('.')])
        return clone

    async def touch(self):
        item = await self.item
        await item.touch()

    async def update(self):
        if self.__snapshot__:
            await context.txn.upsert(self)
        item = await self.item
        await item.update()

    async def remove(self):
        exploded_path = self.path.split('.')
        comp_name = exploded_path[-1]
        parent = await self.parent
        comp = getattr(parent, comp_name)
        if hasattr(comp, 'remove'):
            # composition
            await comp.remove(self)
        else:
            # embedded
            setattr(parent, comp_name, None)
            await context.txn.upsert(parent)

    # HTTP views
    def get(self, _):
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, request):
        self.reset()
        try:
            await self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, request):
        try:
            await self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self

    @db.transactional()
    async def delete(self, _):
        await self.remove()
