from porcupine import context, db, view
from porcupine.contract import contract
from porcupine.datatypes import String
from porcupine import exceptions
from porcupine.core.context import system_override
from .item import GenericItem
from .container import Container
from .mixins import Removable


class DeletedItem(GenericItem, Removable):
    del_id = String(readonly=True, required=True)
    location = String(readonly=True, required=True)
    name = String(required=True, unique=False)

    def __init__(self, dict_storage=None, deleted_item=None):
        super().__init__(dict_storage)
        if deleted_item is not None:
            self.name = deleted_item.name
            self.del_id = deleted_item.id

    async def deleted_item(self):
        with system_override():
            return await db.get_item(self.del_id)

    async def append_to(self, recycle_bin):
        if not context.system_override:
            raise exceptions.InvalidUsage(
                'Cannot directly append this item. '
                'Use the "recycle" method instead.')
        self._append_to(recycle_bin)

    async def restore(self):
        deleted_item = await self.deleted_item()
        with system_override():
            deleted_item.deleted -= 1
        context.txn.upsert(deleted_item)
        await self.remove()

    @contract(accepts=bool)
    @db.transactional()
    async def restored(self, request):
        if request.json:
            await self.restore()
            return True
        return False
    restored = view(put=restored)

    @db.transactional()
    async def delete(self, request):
        deleted_item = await self.deleted_item()
        if deleted_item is not None:
            await deleted_item.remove()
        await self.remove()
        return True


class RecycleBin(Container):
    containment = (DeletedItem, )
