from porcupine import context, db, view
from porcupine.contract import contract
from porcupine.datatypes import String
from porcupine import exceptions
from porcupine.core.context import system_override
from .item import GenericItem
from .container import Container


class DeletedItem(GenericItem):
    location = String(readonly=True, required=True)
    name = String(required=True, unique=False)

    def __init__(self, dict_storage=None, deleted_item=None):
        super().__init__(dict_storage)
        if deleted_item is not None:
            self.name = deleted_item.name
            with system_override():
                # make sure each item is recycled once
                self.id = 'del:{0}'.format(deleted_item.id)

    async def deleted_item(self):
        with system_override():
            return await db.get_item(self.id.split(':')[1])

    async def append_to(self, recycle_bin):
        if not context.system_override:
            raise exceptions.InvalidUsage(
                'Cannot directly append this item. '
                'Use the "recycle" method instead.')
        self._append_to(recycle_bin)

    async def restore(self):
        deleted_item = await self.deleted_item()
        with system_override():
            deleted_item.deleted = False
        await deleted_item.update()
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
        await super().delete(request)
        return True


class RecycleBin(Container):
    containment = (DeletedItem, )
