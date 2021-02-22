from sanic.response import json

from porcupine import db, exceptions, pipe
from porcupine.view import view
from porcupine.core.datatypes.system import Items, Containers
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
    containment = (Item, )
    items = Items()
    containers = Containers()

    indexes = ('is_collection', )

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

    async def children_count(self):
        container_views = db_connector().views[Container]
        cursor = container_views['is_collection'].get_cursor()
        cursor.set_scope(self.id)
        return await cursor.count()

    async def items_count(self):
        container_views = db_connector().views[Container]
        cursor = container_views['is_collection'].get_cursor()
        cursor.set_scope(self.id)
        cursor.set([False])
        return await cursor.count()

    async def containers_count(self):
        container_views = db_connector().views[Container]
        cursor = container_views['is_collection'].get_cursor()
        cursor.set_scope(self.id)
        cursor.set([True])
        return await cursor.count()

    async def get_child_by_name(self, name, resolve_shortcut=False):
        """
        This method returns the child with the specified name.

        @param name: The name of the child
        @type name: str
        @param resolve_shortcut: Return the shortcut's target if child is
            shortcut
        @type resolve_shortcut: bool
        @return: The child object if a child with the given name exists
                 else None.
        @rtype: L{GenericItem}
        """
        child_id = await db_connector().get_raw(
                utils.get_key_of_unique(self.id, 'name', name))
        if child_id:
            item = await db.get_item(child_id)
            if resolve_shortcut and isinstance(item, Shortcut):
                item = await item.get_target()
            return item

    async def get_child_by_id(self, oid):
        item = await db.get_item(oid)
        if item is not None and item.parent_id != self.id:
            return None
        return item

    async def get_children(self, skip=0, take=None,
                           resolve_shortcuts=False) -> list:
        """
        This method returns all the children of the container.

        @rtype: list
        """
        feeder = self.containers.items() | pipe.chain(
            self.items.items(resolve_shortcuts=resolve_shortcuts)
        )
        if skip or take:
            feeder |= pipe.skip_and_take(skip, take)
        return [i async for i in feeder]

    async def get_items(self, skip=0, take=None,
                        resolve_shortcuts=False) -> list:
        """
        This method returns the children that are not containers.

        @rtype: list
        """
        items = self.items.items(resolve_shortcuts=resolve_shortcuts)
        if skip or take:
            items |= pipe.skip_and_take(skip, take)
        return [i async for i in items]

    async def get_containers(self, skip=0, take=None) -> list:
        """
        This method returns the children that are containers.

        @rtype: list
        """
        containers = self.containers.items()
        if skip or take:
            containers |= pipe.skip_and_take(skip, take)
        return [i async for i in containers]

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
