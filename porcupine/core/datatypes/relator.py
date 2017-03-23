"""
Porcupine reference data types
==============================
"""
from porcupine import db, exceptions
# from .common import String
# from porcupine.core.datatypes.mutable import List
from .reference import Reference1, ReferenceN, ItemCollection, \
    ItemReference
from .datatype import DataType
# from porcupine.core.objectSet import ObjectSet
from porcupine.utils import system


# class SingleReference(str):
#
#     def get_item(self):
#         """
#         This method returns the object that this data type
#         instance references. If the current user has no read
#         permission on the referenced item or it has been deleted
#         then it returns None.
#
#         @rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
#         @return: The referenced object, otherwise None
#         """
#         item = None
#         if self:
#             item = db.get_item(self)
#         return item
#
#
# class Reference1(String):
#     """
#     This data type is used whenever an item losely references
#     at most one other item. Using this data type, the referenced item
#     B{IS NOT} aware of the items that reference it.
#
#     @cvar relates_to: a list of strings containing all the permitted content
#                     classes that the instances of this type can reference.
#     """
#     safe_type = str
#     allow_none = True
#     relates_to = ()
#
#     def __init__(self, default=None, **kwargs):
#         super(Reference1, self).__init__(default, **kwargs)
#         if 'relates_to' in kwargs:
#             self.relates_to = kwargs['relates_to']
#
#     def __get__(self, instance, owner):
#         if instance is None:
#             return self
#         value = super(Reference1, self).__get__(instance, owner)
#         if value:
#             value = SingleReference(value)
#             return value
#
#     def clone(self, instance, memo):
#         if '_id_map_' in memo:
#             value = super(Reference1, self).__get__(
#                 instance, instance.__class__)
#             super(Reference1, self).__set__(
#                 instance, memo['_id_map_'].get(value, value))
#
#
# class MultiReference(object):
#     def __init__(self, id_list):
#         self.__value = id_list
#
#     def __getattr__(self, item):
#         return getattr(self.__value, item)
#
#     def __len__(self):
#         return len(self.__value)
#
#     def __nonzero__(self):
#         return len(self.__value)
#
#     def __getitem__(self, key):
#         return self.__value[key]
#
#     def __add__(self, other):
#         return self.__value + other
#
#     def _fetch(self, get_lock):
#         items = []
#         top_level = [oid for oid in self.__value if '.' not in oid]
#         items += db.get_multi(top_level, get_lock=get_lock)
#         embedded = [oid for oid in self.__value if '.' in oid]
#         items += filter(None, [db.get_item(oid, get_lock) for oid in embedded])
#         return items
#
#     def get_items(self, get_lock=True):
#         """
#         This method returns the items that this
#         instance references.
#
#         @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
#         """
#         return ObjectSet(self._fetch(get_lock))
#
#
# class ReferenceN(List):
#     """
#     This data type is used whenever an item losely references
#     none, one or more than one items. Using this data type,
#     the referenced items B{ARE NOT} aware of the items that reference them.
#
#     @cvar relates_to: a list of strings containing all the permitted content
#                       classes that the instances of this type can reference.
#     """
#     relates_to = ()
#
#     def __init__(self, default=None, **kwargs):
#         if default is None:
#             default = []
#         super(ReferenceN, self).__init__(default, **kwargs)
#         if 'relates_to' in kwargs:
#             self.relates_to = kwargs['relates_to']
#
#     def __get__(self, instance, owner):
#         if instance is None:
#             return self
#         value = super(ReferenceN, self).__get__(instance, owner)
#         return MultiReference(value)
#
#     def clone(self, instance, memo):
#         if '_id_map_' in memo:
#             value = super(ReferenceN, self).__get__(
#                 instance, instance.__class__)
#             super(ReferenceN, self).__set__(
#                 instance, [memo['_id_map_'].get(oid, oid) for oid in value])


class RelatorItemReference(ItemReference):
    descriptor = None

    async def item(self):
        item = await super().item()
        if not item or self.descriptor.rel_attr not in item.__schema__:
            return None
        return item


class Relator1(Reference1):
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
    rel_attr = ''
    cascade_delete = False
    respects_references = False

    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)
        if 'rel_attr' in kwargs:
            self.rel_attr = kwargs['rel_attr']
        if 'cascade_delete' in kwargs:
            self.cascade_delete = kwargs['cascade_delete']
        if 'respects_references' in kwargs:
            self.respects_references = kwargs['respects_references']

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = DataType.__get__(self, instance, owner)
        if not value:
            return None
        value = RelatorItemReference(value)
        value.descriptor = self
        return value

    def on_create(self, instance, value):
        self.on_update(instance, value, None)

    def on_update(self, instance, value, old_value):
        if value != old_value:
            if old_value:
                # remove old reference
                self._remove_reference(old_value, instance.id)
            if value:
                self._add_reference(instance, value, instance.id)

    def on_delete(self, instance, value, is_permanent):
        if not instance._is_deleted:
            if value and self.respects_references:
                raise exceptions.ReferentialIntegrityError(
                    'Cannot delete object "{}" '.format(instance.name) +
                    'because it is referenced by other objects.')
            if self.cascade_delete:
                db._db.get_item(value)._recycle()
        if is_permanent and value:
            if self.cascade_delete:
                db._db.get_item(value)._delete()
            else:
                # remove reference
                self._remove_reference(value, instance.id)

    def on_undelete(self, instance, value):
        if self.cascade_delete:
            db._db.get_item(value)._undelete()

    def _add_reference(self, instance, value, oid):
        ref_item = db._db.get_item(value)
        if ref_item is not None and isinstance(
                ref_item,
                tuple([system.get_rto_by_name(cc) for cc in self.relates_to])):
            ref_attr = getattr(ref_item, self.rel_attr)
            ref_attr_def = ref_item.__props__[self.rel_attr]
            reference_added = False
            if isinstance(ref_attr_def, RelatorN) and oid not in ref_attr:
                ref_attr.append(oid)
                reference_added = True
            elif isinstance(ref_attr_def, Relator1) and oid != ref_attr:
                setattr(ref_item, self.rel_attr, oid)
                reference_added = True
            if reference_added:
                ref_attr_def.validate(ref_item)
                db._db.put_item(ref_item)
        else:
            setattr(instance, self.name, None)

    def _remove_reference(self, value, oid):
        ref_item = db._db.get_item(value)
        if ref_item is not None:
            ref_attr = getattr(ref_item, self.rel_attr)
            ref_attr_def = ref_item.__props__[self.rel_attr]
            reference_removed = False
            if isinstance(ref_attr_def, RelatorN) and oid in ref_attr:
                ref_attr.remove(oid)
                reference_removed = True
            elif isinstance(ref_attr_def, Relator1) and ref_attr is not None:
                setattr(ref_item, self.rel_attr, None)
                reference_removed = True
            if reference_removed:
                ref_attr_def.validate(ref_item)
                db._db.put_item(ref_item)


class RelatorItemCollection(ItemCollection):
    async def items(self):
        items = await super().items()
        return [item for item in items
                if self._descriptor.rel_attr in item.__schema__]


class RelatorN(ReferenceN):
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
    rel_attr = ''
    cascade_delete = False
    respects_references = False

    def __init__(self, default=(), **kwargs):
        super().__init__(default, **kwargs)
        if 'rel_attr' in kwargs:
            self.rel_attr = kwargs['rel_attr']
        if 'cascade_delete' in kwargs:
            self.cascade_delete = kwargs['cascade_delete']
        if 'respects_references' in kwargs:
            self.respects_references = kwargs['respects_references']

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return RelatorItemCollection(self, instance)

    def add_reference(self, instance, item):
        if not self.accepts_item(item):
            raise exceptions.ContainmentError(instance,
                                              self.name, item)
        getattr(item, self.rel_attr).add(instance)

    def remove_reference(self, instance, item):
        getattr(item, self.rel_attr).remove(instance)

    async def on_change(self, instance, value, old_value):
        added, removed = await super().on_change(instance, value, old_value)
        for item in added:
            self.add_reference(instance, item)
        for item in removed:
            self.remove_reference(instance, item)

    def on_delete(self, instance, value, is_permanent):
        if not instance._is_deleted:
            if value and self.respects_references:
                raise exceptions.ReferentialIntegrityError(
                    'Cannot delete object "{}" '.format(instance.name) +
                    'because it is being referenced by other objects.')
            if self.cascade_delete:
                [db._db.get_item(id)._recycle() for id in value]
        if is_permanent:
            if self.cascade_delete:
                [db._db.get_item(id)._delete() for id in value]
            else:
                # remove all references
                self._remove_references(value, instance.id)

    def on_undelete(self, instance, value):
        if self.cascade_delete:
            [db._db.get_item(oid)._undelete() for oid in value]

    # def _add_references(self, attr, ids, oid):
    #     allowed_ref_types = tuple(
    #         [misc.get_rto_by_name(cc) for cc in self.relates_to])
    #     for id in ids:
    #         ref_item = db._db.get_item(id)
    #         if ref_item is not None \
    #                 and isinstance(ref_item, allowed_ref_types):
    #             ref_attr = getattr(ref_item, self.rel_attr)
    #             ref_attr_def = ref_item.__props__[self.rel_attr]
    #             reference_added = False
    #             if isinstance(ref_attr_def, RelatorN) and oid not in ref_attr:
    #                 ref_attr.append(oid)
    #                 reference_added = True
    #             elif isinstance(ref_attr_def, Relator1) and oid != ref_attr:
    #                 setattr(ref_item, self.rel_attr, oid)
    #                 reference_added = True
    #             if reference_added:
    #                 # ref_item._update_schema()
    #                 ref_attr_def.validate(ref_item)
    #                 db._db.put_item(ref_item)
    #         else:
    #             attr.remove(id)
    #
    # def _remove_references(self, ids, oid):
    #     # remove references
    #     for id in ids:
    #         ref_item = db._db.get_item(id)
    #         if ref_item is not None:
    #             ref_attr = getattr(ref_item, self.rel_attr)
    #             ref_attr_def = ref_item.__props__[self.rel_attr]
    #             reference_removed = False
    #             if isinstance(ref_attr_def, RelatorN) and oid in ref_attr:
    #                 ref_attr.remove(oid)
    #                 reference_removed = True
    #             elif isinstance(ref_attr_def, Relator1) \
    #                     and ref_attr is not None:
    #                 setattr(ref_item, self.rel_attr, None)
    #                 reference_removed = True
    #             if reference_removed:
    #                 ref_attr_def.validate(ref_item)
    #                 db._db.put_item(ref_item)
