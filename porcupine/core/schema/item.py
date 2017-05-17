import datetime

from porcupine import context, exceptions, db
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.core.datatypes.system import Acl
from porcupine.core.utils import permissions
from porcupine.datatypes import String, DateTime, Boolean, RelatorN
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
    :type created: str
    :ivar modified: The last modification date, handled by the server.
    :type modified: str
    :cvar name: The display name of the object.
    :type name: L{String<porcupine.dt.String>}
    :cvar description: A short description.
    :type description: str
    """
    # system attributes
    parent_id = String(readonly=True, allow_none=True,
                       default=None, store_as='pid')
    created = DateTime(readonly=True, store_as='cr')
    owner = String(required=True, readonly=True, store_as='own')
    modified_by = String(required=True, readonly=True, store_as='mdby')
    modified = DateTime(required=True, readonly=True, store_as='md')

    # security attributes
    is_system = Boolean(readonly=True, protected=True, store_as='sys')
    # roles_inherited = Boolean(default=True, store_as='ri')
    acl = Acl()

    # common attributes
    name = String(required=True, unique=True)
    description = String(store_as='desc')

    @property
    async def is_stale(self):
        if self.parent_id is not None:
            _, parent_exists = await db.connector.exists(self.__storage__.pid)
            return not parent_exists
        return False

    async def clone(self, memo: dict=None) -> 'GenericItem':
        clone: 'GenericItem' = await super().clone(memo)
        now = datetime.datetime.utcnow().isoformat()
        user = context.user
        with system_override():
            clone.owner = user.id
            clone.created = clone.modified = now
            clone.modified_by = user.name
            clone.parent_id = None
        return clone

    async def append_to(self, parent) -> None:
        """
        Adds the item to the specified container.

        @param parent: The destination container 
        @type parent: L{Container}
        @return: None
        """
        if not self.__is_new__:
            raise exceptions.DBAlreadyExists(
                'Object already exists. Use "copy_to" '
                'or "move_to" methods instead.')

        user = context.user
        if not context.system_override:
            if parent is not None:
                security = parent.__storage__.acl
            else:
                # add as root
                security = {}
            user_role = await permissions.resolve_acl(security, user)
            if user_role == permissions.READER:
                raise exceptions.Forbidden(
                    'The user does not have write permissions '
                    'on the parent container.')

        with system_override():
            self.owner = user.id
            self.created = self.modified = \
                datetime.datetime.utcnow().isoformat()
            self.modified_by = user.name
            if parent is not None:
                self.parent_id = parent.id
            if self.__storage__.acl is None:
                self.__storage__.acl = parent.__storage__.acl

            context.txn.insert(self)
            if parent is not None:
                if self.is_collection:
                    await parent.containers.add(self)
                else:
                    await parent.items.add(self)
                parent.modified = self.modified
                context.txn.upsert(parent)

    async def is_contained_in(self, item: 'GenericItem') -> bool:
        """
        Checks if the item is contained in the specified container.

        @param item: The item to check against
        @type item: Elastic
        @rtype: bool
        """
        parent = await self.get_parent()
        while parent:
            if parent.id == item.id:
                return True
            parent = await parent.get_parent()
        return False

    async def get_parent(self) -> Elastic:
        """
        Returns the parent container

        @return: the parent container object
        @rtype: type
        """
        if self.parent_id is not None:
            return await db.get_item(self.parent_id)

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
        while parent.parent_id is not None:
            parent = await parent.get_parent()
            if parent is not None:
                parents.insert(0, parent)
            else:
                # user has no access to parent
                break
        return parents

    async def full_path(self, include_self=True):
        parents = await self.get_all_parents()
        if include_self and self.parent_id is not None:
            parents.append(self)
        path = '/'.join([p.name for p in parents])
        return '/%s' % path if not path.startswith('/') else path

    async def update(self) -> None:
        """
        Updates the item.

        @return: None
        """
        if self.__snapshot__:
            if self.parent_id is not None:
                parent = await db.connector.get(self.parent_id)
            else:
                parent = None

            user = context.user
            user_role = await permissions.resolve(self, user)

            if user_role < permissions.AUTHOR:
                raise exceptions.Forbidden('Forbidden')

            with system_override():
                self.modified_by = user.name
                self.modified = datetime.datetime.utcnow().isoformat()
                context.txn.upsert(self)
                if parent is not None:
                    parent.modified = self.modified
                    context.txn.upsert(parent)

    # HTTP views
    def get(self, _):
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def put(self, request):
        self.reset()
        try:
            self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, request):
        try:
            self.apply_patch(request.json)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        await self.update()
        return self


class Item(Cloneable, Movable, Recyclable, GenericItem):
    """
    Simple item with shortcuts.

    Normally, this is the base class of your custom Porcupine Objects.
    Subclass the L{porcupine.schema.Container} class if you want
    to create custom containers.
    """
    shortcuts = RelatorN(
        accepts=('porcupine.schema.Shortcut', ),
        rel_attr='target',
        cascade_delete=True,
    )
