"""
Porcupine reference data types
==============================
"""
from porcupine import exceptions, db, context
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

    def add_reference(self, instance, item):
        if not self.accepts_item(item):
            raise exceptions.ContainmentError(instance,
                                              self.name, item)
        rel_attr_value = getattr(item, self.rel_attr)
        if isinstance(rel_attr_value, RelatorCollection):
            # call super add to avoid recursion
            super(RelatorCollection, rel_attr_value).add(instance)
        elif isinstance(rel_attr_value, RelatorItem):
            setattr(item, self.rel_attr, instance.id)
            # if not item.__is_new__:
            context.txn.upsert(item)

    def remove_reference(self, instance, item):
        rel_attr_value = getattr(item, self.rel_attr)
        if isinstance(rel_attr_value, RelatorCollection):
            # call super remove to avoid recursion
            super(RelatorCollection, rel_attr_value).remove(instance)
        elif isinstance(rel_attr_value, RelatorItem):
            setattr(item, self.rel_attr, None)
            # if not item.__is_new__:
            context.txn.upsert(item)
            # item.__storage__[self.rel_attr] = None


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

    async def on_change(self, instance, value, old_value):
        ref_item = await super().on_change(instance, value, old_value)
        if ref_item:
            self.add_reference(instance, ref_item)
        if old_value:
            old_ref_item = await db.connector.get(old_value)
            if old_ref_item:
                self.remove_reference(instance, old_ref_item)

    # def on_delete(self, instance, value, is_permanent):
    #     if not instance._is_deleted:
    #         if value and self.respects_references:
    #             raise exceptions.ReferentialIntegrityError(
    #                 'Cannot delete object "{}" '.format(instance.name) +
    #                 'because it is referenced by other objects.')
    #         if self.cascade_delete:
    #             db._db.get_item(value)._recycle()
    #     if is_permanent and value:
    #         if self.cascade_delete:
    #             db._db.get_item(value)._delete()
    #         else:
    #             # remove reference
    #             self._remove_reference(value, instance.id)
    #
    # def on_undelete(self, instance, value):
    #     if self.cascade_delete:
    #         db._db.get_item(value)._undelete()


class RelatorCollection(ItemCollection):
    async def items(self):
        items = await super().items()
        return [item for item in items
                if self._desc.rel_attr in item.__schema__]

    def add(self, item):
        super().add(item)
        self._desc.add_reference(self._inst, item)

    def remove(self, item):
        super().remove(item)
        self._desc.remove_reference(self._inst, item)


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
    storage_info = '_relN_'

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)
        RelatorBase.__init__(self, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return RelatorCollection(self, instance)

    async def on_change(self, instance, value, old_value):
        added, _ = await super().on_change(instance, value, old_value)
        if instance.__is_new__:
            for item in added:
                self.add_reference(instance, item)

    # def on_delete(self, instance, value, is_permanent):
    #     if not instance._is_deleted:
    #         if value and self.respects_references:
    #             raise exceptions.ReferentialIntegrityError(
    #                 'Cannot delete object "{}" '.format(instance.name) +
    #                 'because it is being referenced by other objects.')
    #         if self.cascade_delete:
    #             [db._db.get_item(id)._recycle() for id in value]
    #     if is_permanent:
    #         if self.cascade_delete:
    #             [db._db.get_item(id)._delete() for id in value]
    #         else:
    #             # remove all references
    #             self._remove_references(value, instance.id)
    #
    # def on_undelete(self, instance, value):
    #     if self.cascade_delete:
    #         [db._db.get_item(oid)._undelete() for oid in value]
