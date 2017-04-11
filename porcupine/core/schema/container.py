import itertools
from sanic.response import json
from porcupine import db, view, gather, contract, exceptions, server
from porcupine.core.datatypes.system import Items, Containers
from porcupine.utils import permissions, system
from .shortcut import Shortcut
from .item import Item


class Container(Item):
    """
    Generic container class.

    Base class for all containers.

    @cvar containment: a tuple of strings with all the content types of
                       Porcupine objects that this class instance can accept.
    @type containment: tuple
    """
    containment = (Shortcut, )
    items = Items()
    containers = Containers()

    def __init__(self, dict_storage=None):
        super().__init__(dict_storage)
        self.__storage__.col = True

    async def child_exists(self, name):
        """
        Checks if a child with the specified name is contained
        in the container.

        @param name: The name of the child to check for
        @type name: str

        @rtype: bool
        """
        child_id = await db.connector.get_raw(
                system.get_key_of_unique(self.id, 'name', name))
        if child_id is None:
            return False
        else:
            return True

    async def get_child_by_name(self, name, resolve_shortcuts=False):
        """
        This method returns the child with the specified name.

        @param name: The name of the child
        @type name: str
        @return: The child object if a child with the given name exists
                 else None.
        @rtype: L{GenericItem}
        """
        child_id = await db.connector.get_raw(
                system.get_key_of_unique(self.id, 'name', name))
        if child_id:
            item = await db.get_item(child_id)
            if resolve_shortcuts and isinstance(item, Shortcut):
                item = item.get_target()
            return item

    async def get_child_by_id(self, oid):
        item = await db.get_item(oid)
        if item is not None:
            if item.parent_id != self.id:
                return None
        return item

    async def get_children(self, resolve_shortcuts=False):
        """
        This method returns all the children of the container.

        @rtype: L{ObjectSet<porcupine.core.objectset.ObjectSet>}
        """
        return itertools.chain(*await gather(self.get_containers(),
                                             self.get_items()))

    async def get_items(self, resolve_shortcuts=False):
        """
        This method returns the children that are not containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        return await self.items.items()

    async def get_containers(self, resolve_shortcuts=False):
        """
        This method returns the children that are containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        return await self.containers.items()

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

    @view
    async def children(self, request):
        return await self.get_children()

    @children.http_post
    @contract.is_new_item()
    @db.transactional()
    async def children(self, request):
        item_dict = request.json
        # TODO: handle invalid type exception
        item_type = system.get_rto_by_name(item_dict.pop('type'))
        new_item = item_type()
        try:
            for attr, value in item_dict.items():
                setattr(new_item, attr, value)
            await new_item.append_to(self)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        location = server.url_for('resources.resource_handler',
                                  item_id=new_item.id)
        return json(
            new_item.id,
            status=201,
            headers={
                'Location': location
            })

