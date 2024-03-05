from porcupine import db
from porcupine.view import view
from porcupine.contract import contract
from porcupine.datatypes import String
from porcupine.core.context import system_override, context
from .item import GenericItem
from .container import Container


class DeletedItem(GenericItem):
    location = String(immutable=True, required=True)

    def __init__(self, dict_storage=None, deleted_item=None, **kwargs):
        super().__init__(dict_storage, **kwargs)
        if self.__is_new__ and not context.system_override:
            raise TypeError('DeletedItem objects cannot be instantiated')
        if deleted_item is not None:
            self.name = deleted_item.name
            # make sure each item is recycled once
            self.__storage__.id = 'del:{0}'.format(deleted_item.id)

    async def deleted_item(self):
        with system_override():
            return await db.get_item(self.id.split(':')[1])

    async def append_to(self, recycle_bin):
        if not context.system_override:
            raise TypeError(
                'Cannot directly append this item. '
                'Use the "recycle" method instead.')
        await super().append_to(recycle_bin)

    async def restore(self):
        deleted_item = await self.deleted_item()
        await deleted_item.restore()
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
    # unique_constraints = ()
    containment = DeletedItem,
    indexes = ()
