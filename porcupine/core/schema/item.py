import datetime

from porcupine import context, exceptions, db, view
from porcupine.contract import contract
from porcupine.datatypes import String, DateTime, Boolean, RelatorN, Counter
from porcupine.core.datatypes.system import Acl, Deleted
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
    is_collection = False

    # system attributes
    created = DateTime(readonly=True, store_as='cr')
    owner = String(required=True, readonly=True, store_as='own')
    modified_by = String(required=True, readonly=True, store_as='mdby')
    is_system = Boolean(readonly=True, store_as='sys')
    modified = DateTime(required=True, readonly=True, store_as='md')
    deleted = Deleted(readonly=True, store_as='dl')

    # common attributes
    name = String(required=True, unique=True)
    description = String(store_as='desc')
    acl = Acl(default=None)

    async def is_deleted(self):
        if self.deleted or self.parent_id is None:
            return self.deleted
        return await resolve_deleted(self.parent_id)

    async def applied_acl(self):
        acl = self.get_snapshot_of('acl')
        if acl is not None or self.parent_id is None:
            return acl
        return await resolve_acl(self.parent_id)

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
            security = await parent.applied_acl()
        else:
            # add as root
            security = {}

        user = context.user
        user_role = await permissions.resolve_acl(security, user)
        if user_role == permissions.READER:
            raise exceptions.Forbidden(
                'The user does not have write permissions '
                'on the parent container.')
        self._append_to(parent)

    def _append_to(self, parent):
        user = context.user
        with system_override():
            self.owner = user.id
            self.created = self.modified = \
                datetime.datetime.utcnow().isoformat()
            self.modified_by = user.name
            if parent is not None:
                self.parent_id = parent.id

            context.txn.insert(self)
            if parent is not None:
                if self.is_collection:
                    parent.containers.add(self)
                else:
                    parent.items.add(self)
                parent.modified = self.modified
                context.txn.upsert(parent)

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


class Item(GenericItem):
    """
    Simple item with update capability.

    Normally, this is the base class of your custom Porcupine Objects.
    Subclass the L{porcupine.schema.Container} class if you want
    to create custom containers.
    """
    shortcuts = RelatorN(
        accepts=('porcupine.schema.Shortcut', ),
        rel_attr='target',
        cascade_delete=True,
    )

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

            if user_role > permissions.READER:
                with system_override():
                    self.modified_by = user.name
                    self.modified = datetime.datetime.utcnow().isoformat()
                    context.txn.upsert(self)
                    if parent is not None:
                        parent.modified = self.modified
                        context.txn.upsert(parent)
            else:
                raise exceptions.Forbidden('Forbidden')

    # HTTP views
    @contract(accepts=bool)
    @db.transactional()
    async def recycled(self, request):
        if request.json:
            with system_override():
                recycle_bin = await db.get_item('RB')
            await self.recycle_to(recycle_bin)
            return True
        return False
    recycled = view(put=recycled)

    def get(self, request):
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, request):
        for attr, value in request.json.items():
            try:
                setattr(self, attr, value)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        await self.update()

    @db.transactional()
    async def delete(self, request):
        await self.remove()
        return True
