from porcupine.core.schema.shortcut import Shortcut
from porcupine.datatypes import ItemCollection
from porcupine.core.datatypes.system import Children
from porcupine.utils import permissions
from .item import Item


class Container(Item):
    """
    Generic container class.

    Base class for all containers.

    @cvar containment: a tuple of strings with all the content types of
                       Porcupine objects that this class instance can accept.
    @type containment: tuple
    @type is_collection: bool
    """
    containment = ('porcupine.schema.Shortcut',)
    is_collection = True

    children = Children()
    containers = ItemCollection(readonly=True)

    def child_exists(self, name):
        """
        Checks if a child with the specified name is contained
        in the container.

        @param name: The name of the child to check for
        @type name: str

        @rtype: bool
        """
        item_id = db._db.get_child_id_by_name(self._id, name)
        if item_id is None:
            return False
        else:
            return True

    def get_child_by_name(self, name, get_lock=True, resolve_shortcuts=False):
        """
        This method returns the child with the specified name.

        @param name: The name of the child
        @type name: str
        @return: The child object if a child with the given name exists
                 else None.
        @rtype: L{GenericItem}
        """
        item = db._db.get_child_by_name(self._id, name, get_lock)
        if item is not None:
            user_role = permissions.resolve(item, context.user)
            if user_role < permissions.READER:
                return None
        if resolve_shortcuts and isinstance(item, Shortcut):
            item = item.get_target(get_lock=get_lock)
        return item

    def get_child_by_id(self, oid, get_lock=True):
        item = db.get_item(oid, get_lock)
        if item is not None:
            if item.parentid != self.id:
                return None
        return item

    def get_children(self, resolve_shortcuts=False):
        """
        This method returns all the children of the container.

        @rtype: L{ObjectSet<porcupine.core.objectset.ObjectSet>}
        """
        cursor = db._db.get_children(self._id)
        cursor.resolve_shortcuts = resolve_shortcuts
        children = ObjectSet([c for c in cursor])
        cursor.close()
        return children

    def get_items(self, resolve_shortcuts=False):
        """
        This method returns the children that are not containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        conditions = (('is_collection', False), )
        cursor = db._db.query(conditions)
        cursor.set_scope(self._id)
        cursor.resolve_shortcuts = resolve_shortcuts
        items = ObjectSet([i for i in cursor])
        cursor.close()
        return items

    def get_subfolders(self, resolve_shortcuts=False):
        """
        This method returns the children that are containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        conditions = (('is_collection', True), )
        cursor = db._db.query(conditions)
        cursor.set_scope(self._id)
        cursor.resolve_shortcuts = resolve_shortcuts
        subfolders = ObjectSet([f for f in cursor])
        cursor.close()
        return subfolders

    def has_items(self):
        """
        Checks if the container has at least one non-container child.

        @rtype: bool
        """
        return self._ni > 0

    def has_containers(self):
        """
        Checks if the container has at least one child container.

        @rtype: bool
        """
        return self._nc > 0

    @property
    def children_count(self):
        """The total number of the container's children"""
        return self._ni + self._nc

    @property
    def items_count(self):
        """The number of the items contained"""
        return self._ni

    @property
    def containers_count(self):
        """The number of containers contained"""
        return self._nc
