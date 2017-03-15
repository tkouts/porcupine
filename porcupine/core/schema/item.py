import datetime
import asyncio

from porcupine import context, exceptions, db
from porcupine.datatypes import String, DateTime, Boolean, RelatorN
from porcupine.core.datatypes.system import Acl
from porcupine.core.context import system_override
from porcupine.utils import permissions
from porcupine.utils.system import resolve_acl, resolve_deleted
from .elastic import Elastic
from .mixins import Cloneable, Movable, Removable


class GenericItem(Elastic, Cloneable, Movable, Removable):
    """
    Generic Item
    The base class of all Porcupine objects.

    @cvar is_collection: A boolean indicating if the object is a container.
    @type is_collection: bool
    @ivar modified_by: The display name of the last modifier.
    @type modified_by: str
    :ivar acl: The object's security descriptor. This is a dictionary
               whose keys are the users' IDs and the values are the roles.
    @type acl: dict

    @ivar modified: The last modification date, handled by the server.
    @type modified: float
    @ivar name: The display name of the object.
    @type name: L{String<porcupine.dt.String>}
    @ivar description: A short description.
    @type description: L{String<porcupine.dt.String>}
    @type created: float
    """
    # system attributes
    created = DateTime(readonly=True)
    owner = String(required=True, readonly=True)
    modified_by = String(required=True, readonly=True)
    sys = Boolean(readonly=True, protected=True)
    modified = DateTime(required=True, readonly=True)
    deleted = Boolean(readonly=True, protected=True)

    name = String(required=True)
    description = String()
    acl = Acl(default=None)

    @property
    def is_deleted(self):
        if self.deleted or self.p_id is None:
            future = asyncio.Future()
            future.set_result(self.deleted)
            return future
        return resolve_deleted(self.p_id)

    @property
    def applied_acl(self):
        if self.acl is not None or self.p_id is None:
            future = asyncio.Future()
            future.set_result(self.acl)
            return future
        return resolve_acl(self.p_id)

    @property
    def is_system(self):
        return self.sys

    async def append_to(self, parent):
        """
        Adds the item to the specified container.

        @param parent: The id of the destination container or the container
                       itself
        @type parent: str OR L{Container}
        @return: None
        """
        if not self.__is_new__:
            raise exceptions.DBAlreadyExists(
                'Object already exists. Use "copy_to" '
                'or "move_to" methods instead.')

        if parent is not None:
            if isinstance(parent, str):
                parent = await db.connector.get(parent)
            security = await parent.applied_acl
        else:
            # add as root
            security = {}

        user = context.user
        user_role = await permissions.resolve(security, user)
        if user_role == permissions.READER:
            raise exceptions.Forbidden(
                'The user does not have write permissions '
                'on the parent container.')

        self.owner = user.id
        self.created = self.modified = \
            datetime.datetime.utcnow().isoformat()
        self.modified_by = user.name
        if parent is not None:
            self.p_id = parent.id

        context.txn.insert(self)
        if parent is not None:
            with system_override():
                parent.children.add(self)
                if self.is_collection:
                    parent.containers.add(self)
                parent.modified = self.modified

    def is_contained_in(self, item_id: str) -> bool:
        """
        Checks if the item is contained in the specified container.

        @param item_id: The id of the container
        @type item_id: str
        @rtype: bool
        """
        return item_id == self.id or item_id in self._pids

    async def get_parent(self):
        """
        Returns the parent container

        @return: the parent container object
        @rtype: type
        """
        if self.p_id is not None:
            return await db.get_item(self.p_id)

    def get_ancestor(self, n_levels=1):
        """
        Returns the element that is situated n_levels above the base object
        in the hierarchy.
        Raises IndexError if there are not n_levels above the base object.

        @return: the requested object
        @rtype: type
        """
        # TODO: implement
        return db.get_item(self._pids[-n_levels])

    async def get_all_parents(self):
        """
        Returns all the parents of the item traversing the
        hierarchy up to the root folder.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        parents = []
        parent = self
        while parent.p_id is not None:
            parent = await parent.get_parent()
            if parent is not None:
                parents.append(parent)
            else:
                # user has no access to parent
                break
        parents.reverse()
        return ObjectSet(parents)

    async def full_path(self, include_self=True):
        parents = await db.connector.get_multi(self._pids[1:])
        path = '/'.join([p.name for p in parents])
        if include_self and self._pid is not None:
            path = '%s/%s' % (path, self.name)
        return '/%s' % path if not path.startswith('/') else path


class Item(GenericItem):
    """
    Simple item with update capability.

    Normally, this is the base class of your custom Porcupine Objects.
    Subclass the L{porcupine.schema.Container} class if you want
    to create custom containers.
    """
    shortcuts = RelatorN(
        relates_to=('porcupine.schema.Shortcut', ),
        rel_attr='target',
        cascade_delete=True,
    )

    async def update(self) -> None:
        """
        Updates the item.

        @return: None
        """
        if self.__snapshot__:
            security = await self.applied_acl
            if self.p_id is not None:
                parent = await db.connector.get(self.p_id)
            else:
                parent = None

            user = context.user
            user_role = await permissions.resolve(security, user)

            if user_role > permissions.READER:
                with system_override():
                    self.modified_by = user.name
                    self.modified = datetime.datetime.utcnow().isoformat()
                    context.txn.update(self)
                    if parent is not None:
                        parent.modified = self.modified
                        context.txn.update(parent)
            else:
                raise exceptions.Forbidden('Forbidden')
