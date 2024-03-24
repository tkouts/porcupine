from porcupine import db, exceptions
from porcupine.core.context import context, system_override, ctx_db
from porcupine.core.schemaregistry import get_content_class
from .common import String


class Acceptable:

    def __init__(self, accepts, cascade_delete):
        self.accepts_resolved = False
        self.accepts = accepts
        self.cascade_delete = cascade_delete

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            self.accepts = tuple([
                get_content_class(x) if isinstance(x, str) else x
                for x in self.accepts
            ])
            self.accepts_resolved = True
        return self.accepts

    async def accepts_item(self, item) -> bool:
        return isinstance(item, self.allowed_types)


class ItemReference(str):
    async def item(self, quiet=True):
        """
        This method returns the object that this data type
        instance references. If the current user has no read
        permission on the referenced item, or it has been deleted
        then it returns None.

        @rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
        @return: The referenced object, otherwise None
        """
        return await db.get_item(self, quiet=quiet)

    def to_json(self):
        return str(self)


class Reference(String, Acceptable):
    """
    This data type is used whenever an item loosely references
    at most one other item. Using this data type, the referenced item
    B{IS NOT} aware of the items that reference it.
    """
    def __init__(
        self,
        default=None,
        accepts=(),
        cascade_delete=False,
        **kwargs
    ):
        super().__init__(default, allow_none=True, **kwargs)
        Acceptable.__init__(self, accepts, cascade_delete)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None:
            return ItemReference(value)

    async def on_create(self, instance, value):
        await super().on_create(instance, value)
        if value:
            ref_item = await ctx_db.get().get(value)
            if ref_item is None:
                # TODO: change wording
                raise exceptions.InvalidUsage(f'Invalid item {value}.')
            if not await self.accepts_item(ref_item):
                raise exceptions.ContainmentError(instance, self.name, ref_item)
            return ref_item

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        return await self.on_create(instance, value)

    async def on_delete(self, instance, value):
        await super().on_delete(instance, value)
        if value and self.cascade_delete:
            ref_item = await ctx_db.get().get(value)
            if ref_item:
                with system_override():
                    await ref_item.remove()

    async def on_recycle(self, instance, value):
        await super().on_recycle(instance, value)
        if value and self.cascade_delete:
            ref_item = await ctx_db.get().get(value)
            if ref_item:
                with system_override():
                    # mark as deleted
                    ref_item.is_deleted += 1
                    await context.db.txn.upsert(ref_item)
                    await context.db.txn.recycle(ref_item)

    async def on_restore(self, instance, value):
        await super().on_restore(instance, value)
        if value and self.cascade_delete:
            with system_override():
                ref_item = await ctx_db.get().get(value)
                if ref_item:
                    await ref_item.restore()

    # def clone(self, instance, memo):
    #     value = super().__get__(instance, None)
    #     super().__set__(instance, memo['_id_map_'].get(value, value))

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        value = getattr(instance, self.name)
        if value and expand:
            return await value.item()
        return value
