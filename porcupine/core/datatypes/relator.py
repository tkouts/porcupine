"""
Porcupine reference data types
==============================
"""
from porcupine import db, context
from porcupine.core.context import system_override
from .reference import Reference1, ReferenceN, ItemCollection, \
    ItemReference, Acceptable


class RelatorBase(Acceptable):
    cascade_delete = False
    respects_references = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rel_attr = kwargs['rel_attr']
        if 'cascade_delete' in kwargs:
            self.cascade_delete = kwargs['cascade_delete']
        if 'respects_references' in kwargs:
            self.respects_references = kwargs['respects_references']
        self.name = None

    async def add_reference(self, instance, *items):
        with system_override():
            for item in items:
                # if not await self.accepts_item(item):
                #     raise exceptions.ContainmentError(instance,
                #                                       self.name, item)
                rel_attr_value = getattr(item, self.rel_attr)
                if isinstance(rel_attr_value, RelatorCollection):
                    # call super add to avoid recursion
                    await super(RelatorCollection, rel_attr_value).add(instance)
                elif isinstance(rel_attr_value, RelatorItem):
                    setattr(item, self.rel_attr, instance.id)
                    context.txn.upsert(item)

    async def remove_reference(self, instance, *items):
        with system_override():
            for item in items:
                rel_attr_value = getattr(item, self.rel_attr)
                if isinstance(rel_attr_value, RelatorCollection):
                    # call super remove to avoid recursion
                    await super(RelatorCollection, rel_attr_value).remove(instance)
                elif isinstance(rel_attr_value, RelatorItem):
                    setattr(item, self.rel_attr, None)
                    context.txn.upsert(item)


class RelatorItem(ItemReference):
    def __new__(cls, value, descriptor):
        s = super().__new__(cls, value)
        s._desc = descriptor
        return s

    async def item(self):
        item = await super().item()
        if not item or self._desc.rel_attr not in item.__schema__:
            return None
        return item


class Relator1(Reference1, RelatorBase):
    """
    This data type is used whenever an item possibly references another item.
    The referenced item B{IS} aware of the items that reference it.

    @cvar rel_attr: contains the name of the attribute of the referenced
                   content classes. The type of the referenced attribute should
                   be B{strictly} be a L{Relator1} or L{RelatorN}
                   data type for one-to-one and one-to-many relationships
                   respectively.
    @type rel_attr: str

    @var respects_references: if set to C{True} then the object cannot be
                              deleted if there are objects that reference it.
    @type respects_references: bool

    @var cascade_delete: if set to C{True} then all the object referenced
                         will be deleted upon the object's deletion.
    @type cascade_delete: bool
    """
    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)
        RelatorBase.__init__(self, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value:
            return RelatorItem(value, self)

    async def on_create(self, instance, value):
        ref_item = await super().on_create(instance, value)
        if ref_item:
            await self.add_reference(instance, ref_item)

    async def on_change(self, instance, value, old_value):
        super().on_change(instance, value, old_value)
        await self.on_create(instance, value)
        if old_value:
            old_ref_item = await db.connector.get(old_value)
            if old_ref_item:
                await self.remove_reference(instance, old_ref_item)

    async def on_delete(self, instance, value):
        super().on_delete(instance, value)
        if value:
            ref_item = await db.connector.get(value)
            if ref_item:
                if self.cascade_delete:
                    with system_override():
                        await ref_item.remove()
                else:
                    await self.remove_reference(instance, ref_item)


class RelatorCollection(ItemCollection):
    async def items(self):
        items = await super().items()
        return [item for item in items
                if self._desc.rel_attr in item.__schema__]

    async def add(self, *items):
        await super().add(*items)
        await self._desc.add_reference(self._inst, *items)

    async def remove(self, *items):
        await super().remove(*items)
        await self._desc.remove_reference(self._inst, *items)


class RelatorN(ReferenceN, RelatorBase):
    """
    This data type is used whenever an item references none, one or more items.
    The referenced items B{ARE} aware of the items that reference them.

    @cvar rel_attr: the name of the attribute of the referenced
                    content classes.
                    The type of the referenced attribute should be B{strictly}
                    be a subclass of L{Relator1} or L{RelatorN} data types for
                    one-to-many and many-to-many relationships respectively.
    @type rel_attr: str

    @cvar respects_references: if set to C{True} then the object
                               cannot be deleted if there are objects that
                               reference it.
    @type respects_references: bool

    @cvar cascade_delete: if set to C{True} then all the objects referenced
                         will be deleted upon the object's deletion.
    @type cascade_delete: bool
    """
    storage_info_prefix = '_relN_'

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)
        RelatorBase.__init__(self, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return RelatorCollection(self, instance)

    @property
    def storage_info(self):
        return '{0}:{1}'.format(self.storage_info_prefix, self.rel_attr)

    async def on_create(self, instance, value):
        added, _ = await super().on_create(instance, value)
        await self.add_reference(instance, *added)

    async def on_change(self, instance, value, old_value):
        added, removed = await super().on_change(instance, value, old_value)
        await self.add_reference(instance, *added)
        await self.remove_reference(instance, *removed)

    async def on_delete(self, instance, value):
        ref_ids = await self.fetch(instance, set_storage=False)
        async for ref_item in db.connector.get_multi(ref_ids):
            if self.cascade_delete:
                with system_override():
                    await ref_item.remove()
            else:
                await self.remove_reference(instance, ref_item)
        # remove collection documents
        await super().on_delete(instance, value)
