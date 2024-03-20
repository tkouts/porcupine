import time
from typing import List, Optional, Mapping
# from collections import ChainMap
from pendulum import DateTime as PendulumDateTime

from porcupine.hinting import TYPING
from porcupine import exceptions, db
from porcupine.contract import contract
from porcupine.response import no_content
from porcupine.core.accesscontroller import Roles
from porcupine.core.context import system_override, context
from porcupine.core.datatypes.system import Acl, AclValue, ParentId
from porcupine.core.utils import date
from porcupine.connectors.schematables import ItemsTable
from porcupine.core.accesscontroller import (
    resolve_acl,
    is_contained_in,
    get_ancestor_id
)
from porcupine.datatypes import (
    String,
    Boolean,
    RelatorN,
    DateTime,
    Integer
)
from .elastic import Elastic
from .mixins import Cloneable, Movable, Removable, Recyclable


class GenericItem(Removable, Elastic):
    """
    Generic Item
    The base class of all Porcupine objects.

    :ivar modified_by: The display name of the last modifier.
    :type modified_by: str
    :cvar acl: The object's security descriptor. This is a dictionary
               whose keys are the users' IDs and the values are the roles.
    :type acl: dict

    :cvar created: The creation date, handled by the server.
    :type created: L{DateTime<pendulum.DateTime>}
    :ivar modified: The last modification date, handled by the server.
    :type modified: L{DateTime<pendulum.DateTime>}
    :cvar name: The display name of the object.
    :type name: str
    :cvar description: A short description.
    :type description: str
    """

    # system attributes
    parent_id = ParentId()
    created: PendulumDateTime = DateTime(readonly=True)
    modified: PendulumDateTime = DateTime(required=True, readonly=True)
    p_type = String(readonly=True, protected=True)

    # security attributes
    is_system = Boolean(readonly=True, protected=True)
    acl: AclValue = Acl()

    # common attributes
    name = String(required=True)
    owner = String(required=True, default=None, allow_none=True,
                   readonly=True, store_as='own')
    modified_by = String(required=True, readonly=True, store_as='mdby')
    description = String(store_as='desc')

    @classmethod
    def table(cls, collection=None):
        return ItemsTable(collection)

    @property
    def friendly_name(self):
        return f'{self.name}({self.content_class})'

    @property
    def effective_acl(self) -> Mapping:
        return resolve_acl(self)

    async def clone(self, memo: dict = None) -> 'Elastic':
        clone: 'Elastic' = await super().clone(memo)
        now = date.utcnow()
        user = context.user
        with system_override():
            clone.owner = user.id
            clone.created = clone.modified = now
            clone.modified_by = user.name
            clone.parent_id = None
        return clone

    async def is_contained_in(self, item: 'GenericItem') -> bool:
        """
        Checks if the item is contained in the specified container.

        @param item: The item to check against
        @type item: Elastic
        @rtype: bool
        """
        return is_contained_in(self, item)

    async def get_parent(self) -> Optional[TYPING.CONTAINER_CO]:
        """
        Returns the parent container

        @return: the parent container object
        @rtype: type
        """
        if self.parent_id is not None:
            return await db.get_item(self.parent_id)

    async def get_ancestor(
        self,
        n_levels: int = 1
    ) -> Optional[TYPING.CONTAINER_CO]:
        """
        Returns the element that is situated n_levels above the base object
        in the hierarchy.
        Raises IndexError if there are not n_levels above the base object.

        @return: the requested object
        @rtype: type
        """
        ancestor_id = get_ancestor_id(self, n_levels)
        if ancestor_id:
            return await db.get_item(ancestor_id)

    async def get_all_parents(self) -> List[TYPING.CONTAINER_CO]:
        """
        Returns all the parents of the item traversing the
        hierarchy up to the root folder.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        parents = []
        parent = self
        while parent.parent_id is not None:
            parent = await parent.get_parent()
            if parent is not None:
                parents.insert(0, parent)
            else:
                # user has no access to parent
                break
        return parents

    async def full_path(self, include_self=True) -> str:
        parents = await self.get_all_parents()
        if include_self and self.parent_id is not None:
            parents.append(self)
        path = '/'.join([p.name for p in parents])
        return '/%s' % path if not path.startswith('/') else path

    async def append_to(self, parent) -> None:
        """
        Adds the item to the specified container.

        @param parent: The destination container 
        @type parent: L{Container}
        @return: None
        """
        if parent is not None:
            await parent.children.add(self)
        else:
            # ROOT item
            if not self.__is_new__:
                raise exceptions.DBAlreadyExists('Object already exists')
            user = context.user
            with system_override():
                self.owner = user.id
                self.created = self.modified = date.utcnow()
                self.modified_by = user.name
            await context.db.txn.insert(self)

    def touch(self) -> None:
        return context.db.txn.touch(self)

    async def update(self) -> bool:
        """
        Updates the item.

        @return: None
        """
        if self.__snapshot__:
            user = context.user
            db_ = context.db
            if not await self.can_update(user):
                raise exceptions.Forbidden('Forbidden')

            if self.parent_id is not None:
                parent = await db_.get(self.parent_id)
                await parent.touch()

            with system_override():
                self.modified_by = user.name
                self.modified = date.utcnow()

            # await self.touch()
            await db_.txn.upsert(self)
            return True
        return False

    # permissions providers
    async def can_read(self, membership):
        if context.system_override:
            return True
        user_role = await Roles.resolve(self, membership)
        return user_role > Roles.NO_ACCESS

    async def can_update(self, membership) -> bool:
        if context.system_override:
            return True
        user_role = await Roles.resolve(self, membership)
        return user_role >= Roles.AUTHOR

    async def can_delete(self, membership) -> bool:
        if self.is_system:
            return False
        elif context.system_override:
            return True
        user_role = await Roles.resolve(self, membership)
        return (
            user_role > Roles.AUTHOR or
            user_role == Roles.AUTHOR and self.owner == membership.id
        )

    # HTTP views
    def get(self, _):
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, request):
        self.reset()
        try:
            await self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, request):
        try:
            await self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self

    @db.transactional()
    async def delete(self, _):
        await self.remove()
        return no_content()


class BaseItem(Cloneable, Movable, Recyclable, GenericItem):
    """
    Simple item with shortcuts.

    Normally, this is the base class of your custom Porcupine Objects.
    Subclass the L{porcupine.schema.Container} class if you want
    to create custom containers.
    """
    shortcuts = RelatorN(
        accepts=('Shortcut', ),
        rel_attr='target',
        cascade_delete=True,
    )


class Item(BaseItem):
    expires_at = Integer(None, immutable=True, allow_none=True, protected=True)

    def expires_after(self, days=0, hours=0, minutes=0, seconds=0):
        ts = time.time()
        if days:
            ts += days * 86400
        if hours:
            ts += hours * 3600
        if minutes:
            ts += minutes * 60
        if seconds:
            ts += seconds
        self.expires_at = int(ts)
