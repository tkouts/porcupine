import itertools

from sanic.response import json

from porcupine import db, exceptions
from porcupine.view import view
from porcupine.core.datatypes.system import Items, Containers
from porcupine.core.aiolocals.local import wrap_gather as gather
from porcupine.core.services import db_connector
from porcupine.core import utils
from .item import Item
from .shortcut import Shortcut


class Container(Item):
    """
    Generic container class.

    Base class for all containers.

    @cvar containment: a tuple of strings with all the content types of
                       Porcupine objects that this class instance can accept.
    @type containment: tuple
    """
    is_collection = True
    containment = ()
    items = Items()
    containers = Containers()

    async def child_exists(self, name: str) -> bool:
        """
        Checks if a child with the specified name is contained
        in the container.

        @param name: The name of the child to check for
        @type name: str

        @rtype: bool
        """
        unique_name_key = utils.get_key_of_unique(self.id, 'name', name)
        _, exists = await db_connector().exists(unique_name_key)
        return exists

    async def get_child_by_name(self, name, resolve_shortcuts=False):
        """
        This method returns the child with the specified name.

        @param name: The name of the child
        @type name: str
        @return: The child object if a child with the given name exists
                 else None.
        @rtype: L{GenericItem}
        """
        child_id = await db_connector().get_raw(
                utils.get_key_of_unique(self.id, 'name', name))
        if child_id:
            item = await db.get_item(child_id)
            if resolve_shortcuts and isinstance(item, Shortcut):
                item = await item.get_target()
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
        return itertools.chain(
            *await gather(self.get_containers(),
                          self.get_items(resolve_shortcuts)))

    async def get_items(self, resolve_shortcuts=False):
        """
        This method returns the children that are not containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        if resolve_shortcuts:
            items = []
            async for item in self.items.items():
                if isinstance(item, Shortcut):
                    item = await item.get_target()
                if item:
                    items.append(item)
            return items
        return [i async for i in self.items.items()]

    async def get_containers(self):
        """
        This method returns the children that are containers.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        return [i async for i in self.containers.items()]

    async def has_items(self) -> bool:
        """
        Checks if the container has at least one non-container child.

        @rtype: bool
        """
        async for _ in self.items.items():
            return True
        return False

    async def has_containers(self) -> bool:
        """
        Checks if the container has at least one child container.

        @rtype: bool
        """
        async for _ in self.containers.items():
            return True
        return False

    # permissions providers
    can_append = Item.can_update

    @view
    async def children(self, _):
        # TODO: add support for resolve_shortcuts
        return await self.get_children()

    @children.http_post
    # @contract.is_new_item()
    @db.transactional()
    async def children(self, request):
        if 'source' in request.args:
            # copy operation
            source = await db.get_item(request.args['source'][0], quiet=False)
            # TODO: handle items with no copy capability
            new_item = await source.copy_to(self)
        else:
            # new item
            try:
                new_item = await Container.new_from_dict(request.json)
                await new_item.append_to(self)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))

        location = '/resources/{0}'.format(new_item.id)

        return json(
            new_item,
            status=201,
            headers={
                'Location': location
            })
