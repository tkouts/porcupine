from typing import Awaitable

from sanic.response import json
from pypika import Parameter

from porcupine import db, exceptions, pipe
from porcupine.view import view
from porcupine.core.datatypes.system import Children
from porcupine.core.services import db_connector
from porcupine.core import utils
from porcupine.connectors.base.bounds import FixedBoundary
from porcupine.core.accesscontroller import AccessRecord
from porcupine.connectors.mutations import Formats
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
    # items = Items()
    # containers = Containers()
    children = Children()

    indexes = ('is_collection', )

    @property
    def access_record(self):
        return AccessRecord(
            self.parent_id,
            self.acl.to_json(),
            self.is_deleted,
            self.expires_at
        )

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

    def children_count(self):
        return self.children.count()
        # container_views = db_connector().views[Container]
        # cursor = container_views['is_collection'].get_cursor()
        # cursor.set_scope(self.id)
        # return await cursor.count()

    def items_count(self):
        return self.children.count(self.children.is_collection == False)
        # container_views = db_connector().views[Container]
        # cursor = container_views['is_collection'].get_cursor()
        # cursor.set_scope(self.id)
        # cursor.set([FixedBoundary(False)])
        # return await cursor.count()

    def containers_count(self):
        return self.children.count(self.children.is_collection == True)
        # container_views = db_connector().views[Container]
        # cursor = container_views['is_collection'].get_cursor()
        # cursor.set_scope(self.id)
        # cursor.set([FixedBoundary(True)])
        # return await cursor.count()

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
        result = await db_connector().query(
            'select * from items where parent_id=? and name=? limit 1',
            [self.id, name]
        )
        # print(len(result))
        if len(result) > 0:
            item = result[0]
            # print(item)
            if resolve_shortcut and isinstance(item, Shortcut):
                item = await item.get_target()
            return item
        # child_id = await db_connector().get(
        #     utils.get_key_of_unique(self.id, 'name', name),
        #     fmt=Formats.STRING
        # )
        # if child_id:
        #     item = await db.get_item(child_id)
        #     if resolve_shortcut and isinstance(item, Shortcut):
        #         item = await item.get_target()
        #     return item

    async def get_child_by_id(self, oid):
        item = await db.get_item(oid)
        if item is not None and item.parent_id != self.id:
            return None
        return item

    def get_children(self, skip=0, take=None,
                     resolve_shortcuts=False) -> Awaitable[list]:
        """
        This method returns all the children of the container.

        @rtype: list
        """
        children = self.children.items(skip, take, resolve_shortcuts)
        return children.list()

    def get_items(self, skip=0, take=None,
                  resolve_shortcuts=False) -> Awaitable[list]:
        """
        This method returns the children that are not containers.

        @rtype: list
        """
        items = self.children.items(
            skip, take, self.children.is_collection == False, resolve_shortcuts
        )
        return items.list()

    def get_containers(self, skip=0, take=None) -> Awaitable[list]:
        """
        This method returns the children that are containers.

        @rtype: list
        """
        containers = self.children.items(
            skip, take, self.children.is_collection == True
        )
        return containers.list()

    async def has_items(self) -> bool:
        """
        Checks if the container has at least one non-container child.

        @rtype: bool
        """
        return not await self.items.items().is_empty()

    async def has_containers(self) -> bool:
        """
        Checks if the container has at least one child container.

        @rtype: bool
        """
        return not await self.containers.items().is_empty()

    # permissions providers
    can_append = Item.can_update

    @view
    async def items(self, _):
        # TODO: add support for resolve_shortcuts
        print(await self.children.count())
        return await self.get_items()

    @view
    async def containers(self, _):
        return await self.get_containers()
