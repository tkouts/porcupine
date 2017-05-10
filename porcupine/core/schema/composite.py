from porcupine import db, context
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
    def __init__(self, dict_storage=None):
        super().__init__(dict_storage)
        if self.__is_new__ and not context.system_override:
            raise TypeError('Composite objects cannot be instantiated')

    @property
    def item_id(self):
        return self.id.split('.')[0]

    @property
    async def is_stale(self):
        return not await db.connector.exists(self.item_id)

    @property
    async def item(self):
        return await db.connector.get(self.item_id)

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

    async def update(self):
        context.txn.upsert(self)
        item = await self.item
        await item.update()
