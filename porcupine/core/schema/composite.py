from porcupine import db, exceptions
from porcupine.contract import contract
from porcupine.datatypes import String, Composition
from porcupine.core.context import context
from .elastic import Elastic
from .item import GenericItem


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

    # these are initialized by schema registry
    embedded_in = None
    collection_name = None

    parent_id = String(
        required=True,
        readonly=True,
        protected=True,
        immutable=True
    )
    p_type = String(readonly=True, protected=True, required=True)

    @classmethod
    def table_name(cls):
        composition = getattr(cls.embedded_in, cls.collection_name)
        return composition.allowed_types[0].__name__.lower()

    # @property
    # async def effective_acl(self):
    #     item = await self.item
    #     return item.effective_acl

    @property
    async def item(self):
        parent = await self.parent
        while not isinstance(parent, GenericItem):
            parent = await parent.parent
        return parent

    @property
    def parent(self):
        return context.db.get(
            self.parent_id,
            _table=self.embedded_in.table_name()
        )

    # @property
    # async def ttl(self):
    #     item = await self.item
    #     return item.expires_at

    # async def clone(self, memo: dict = None) -> 'Composite':
    #     clone: 'Composite' = await super().clone(memo)
    #     with system_override():
    #         id_map = memo['_id_map_']
    #         clone.path = '.'.join([id_map.get(oid, oid)
    #                                for oid in self.path.split('.')])
    #     return clone

    async def touch(self):
        item = await self.item
        await item.touch()

    async def update(self) -> bool:
        item = await self.item
        updated = False
        if self.__snapshot__:
            # prop_name = self.property_name
            # data_type = item.__schema__[prop_name]
            # if isinstance(data_type, ReferenceN):
            #     # composition
            await context.db.txn.upsert(self)
            # else:
            #     # embedded
            #     setattr(item, prop_name, self)
            updated = True
        await item.update()
        return updated

    async def remove(self):
        comp_name = self.collection_name
        parent = await self.parent
        dt = getattr(self.embedded_in, comp_name)
        if isinstance(dt, Composition):
            comp = getattr(parent, comp_name)
            # composition
            await comp.remove(self)
        else:
            # embedded
            # await comp.reset(None)
            setattr(parent, comp_name, None)
            await parent.update()
            # setattr(parent, comp_name, None)
            # await context.txn.upsert(parent)

    # permissions providers
    async def can_read(self, membership) -> bool:
        item = await self.item
        return await item.can_read(membership)

    async def can_update(self, membership) -> bool:
        item = await self.item
        return await item.can_update(membership)

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
