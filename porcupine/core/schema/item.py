import datetime

from porcupine import context, exceptions, db
from porcupine.datatypes import String, DateTime, Boolean, Dictionary
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
    @ivar security: The object's security descriptor. This is a dictionary
                    whose keys are the users' IDs and the values are the roles.
    @type security: dict
    @ivar inherit_roles: Indicates if the object's security
                        descriptor is identical to this of its parent
    @type inherit_roles: bool

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
    created = DateTime(readonly=True)
    owner = String(required=True, readonly=True)
    modified_by = String(required=True, readonly=True)
    sys = Boolean(readonly=True)
    modified = DateTime(required=True, readonly=True)

    name = String(required=True)
    description = String()
    acl = Dictionary(allow_none=True, default=None)

    @property
    def is_deleted(self):
        return resolve_deleted(self)

    @property
    def applied_acl(self):
        return resolve_acl(self)

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
        if self.p_id:
            raise exceptions.DBAlreadyExists(
                'Object already exists. Use update or move_to instead.')

        if parent is not None:
            if isinstance(parent, str):
                parent = await db.connector.get(parent)
            security = await parent.applied_acl
        else:
            # add as root
            security = {}

        content_class = self.content_class

        user = context.user
        user_role = await permissions.resolve(security, user)
        if user_role == permissions.READER:
            raise exceptions.PermissionDenied(
                'The user does not have write permissions '
                'on the parent folder.')
        # set security to new item
        # if parent is not None:
        #     # if content_class not in parent.containment:
        #     #     raise exceptions.ContainmentError(
        #     #         'The target container does not accept '
        #     #         'objects of type\n"%s".' % content_class)
        #
        #     if user_role == permissions.COORDINATOR:
        #         # user is COORDINATOR
        #         self._apply_security(parent, True)
        #     else:
        #         # user is not COORDINATOR
        #         self.inherit_roles = True
        #         self.security = parent.security
        # else:
        #     self.inherit_roles = False

        self.owner = user.id
        self.created = self.modified = \
            datetime.datetime.utcnow().isoformat()
        self.modified_by = user.name
        if parent is not None:
            self.p_id = parent.id
        # self.p_ids = parent.p_ids + [parent.id]

        # db._db.handle_update(self, None)
        context.txn.insert(self)
        if parent is not None:
            with system_override():
                parent.children.add(self.id)
                if self.is_collection:
                    parent.containers.add(self.id)
                parent.modified = self.modified
        # db._db.handle_post_update(self, None)

    def is_contained_in(self, item_id: str) -> bool:
        """
        Checks if the item is contained in the specified container.

        @param item_id: The id of the container
        @type item_id: str
        @rtype: bool
        """
        return item_id == self._id or item_id in self._pids

    def get_parent(self, get_lock=True):
        """
        Returns the parent container

        @return: the parent container object
        @rtype: type
        """
        return db.get_item(self._pid, get_lock=get_lock)

    def get_ancestor(self, n_levels=1, get_lock=True):
        """
        Returns the element that is situated n_levels above the base object in the lookup hierarchy.
        Raises IndexError if there are not n_levels above the base object.

        @return: the requested object
        @rtype: type
        """
        return db.get_item(self._pids[-n_levels], get_lock=get_lock)

    def get_all_parents(self):
        """
        Returns all the parents of the item traversing the
        hierarchy up to the root folder.

        @rtype: L{ObjectSet<porcupine.core.objectSet.ObjectSet>}
        """
        parents = []
        parent = self
        while parent._pid is not None:
            parent = parent.get_parent()
            if parent is not None:
                parents.append(parent)
            else:
                # user has no access to parent
                break
        parents.reverse()
        return ObjectSet(parents)

    # @property
    # def issystem(self):
    #     """Indicates if this is a systemic object
    #
    #     @rtype: bool
    #     """
    #     return self._is_system

    # @property
    # def owner(self):
    #     """The object's creator
    #
    #     @rtype: type
    #     """
    #     return self._owner

    # @property
    # def created(self):
    #     """The creation date
    #
    #     @rtype: float
    #     """
    #     return self._created

    # @property
    # def parentid(self):
    #     """The ID of the parent container
    #
    #     @rtype: str
    #     """
    #     return self._pid

    def full_path(self, include_self=True):
        parents = db._db.get_multi(self._pids[1:])
        path = '/'.join([p.name for p in parents])
        if include_self and self._pid is not None:
            path = '%s/%s' % (path, self.name)
        return '/%s' % path if not path.startswith('/') else path
