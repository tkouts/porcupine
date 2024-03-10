from typing import Awaitable

from pypika import Parameter, Order

from porcupine.view import view
from porcupine.core.datatypes.system import Children
from porcupine.core.accesscontroller import AccessRecord
from .item import Item
from .shortcut import Shortcut
from porcupine.db.index import Index
from porcupine.connectors.postgresql.query import QueryType


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
    children = Children()

    unique_constraints = 'name',

    @staticmethod
    def indexes(t):
        return (
            Index(t.is_collection),
            Index(t.name, unique=True)
        )

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
        q = self.children.query(
            QueryType.RAW,
            where=self.children.name == Parameter(':name')
        )
        q = q.select(self.children.name)
        return await q.execute(first_only=True, name=name) is not None
        # unique_name_key = utils.get_key_of_unique(self.id, 'name', name)
        # _, exists = await db_connector().exists(unique_name_key)
        # return exists

    def children_count(self):
        return self.children.count()

    def items_count(self):
        return self.children.count(self.children.is_collection == False)

    def containers_count(self):
        return self.children.count(self.children.is_collection == True)

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
        q = self.children.query(
            where=self.children.name == Parameter(':name')
        )
        item = await q.execute(first_only=True, name=name)
        if item and resolve_shortcut and isinstance(item, Shortcut):
            item = await item.get_target()
        return item

    def get_child_by_id(self, oid):
        # TODO: maybe use db.get_item to read our own writes?
        return self.children.get_member_by_id(oid)

    def get_children(self, skip=0, take=None, order_by=None, order=Order.asc,
                     resolve_shortcuts=False) -> Awaitable[list]:
        """
        This method returns all the children of the container.

        @rtype: list
        """
        children = self.children.items(skip, take, None, order_by, order,
                                       resolve_shortcuts)
        return children.list()

    def get_items(self, skip=0, take=None,
                  order_by=None, order=Order.asc,
                  resolve_shortcuts=False) -> Awaitable[list]:
        """
        This method returns the children that are not containers.

        @rtype: list
        """
        items = self.children.items(
            skip, take,
            self.children.is_collection == Parameter(':is_collection'),
            order_by, order,
            resolve_shortcuts,
            is_collection=False
        )
        return items.list()

    def get_containers(self, skip=0, take=None,
                       order_by=None, order=Order.asc) -> Awaitable[list]:
        """
        This method returns the children that are containers.

        @rtype: list
        """
        containers = self.children.items(
            skip, take,
            self.children.is_collection == Parameter(':is_collection'),
            order_by, order,
            is_collection=True
        )
        return containers.list()

    # async def has_items(self) -> bool:
    #     """
    #     Checks if the container has at least one non-container child.
    #
    #     @rtype: bool
    #     """
    #     return not await self.items.items().is_empty()
    #
    # async def has_containers(self) -> bool:
    #     """
    #     Checks if the container has at least one child container.
    #
    #     @rtype: bool
    #     """
    #     return not await self.containers.items().is_empty()

    # permissions providers
    can_append = Item.can_update

    @view
    async def items(self, _):
        # TODO: add support for resolve_shortcuts
        # print(await self.children_count())
        # q = self.children.query(QueryType.PARTIAL)
        # q = q.select(self.children.name, self.children.owner)
        # print(q)
        # return await q.cursor(take=20).list()
        return await self.get_items()

    @view
    async def containers(self, _):
        return await self.get_containers()
