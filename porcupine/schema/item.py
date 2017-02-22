import time

from porcupine.core.schema.item import GenericItem
from porcupine.datatypes import RelatorN
from porcupine.utils import permissions


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

    # @db.requires_transactional_context
    def update(self) -> None:
        """
        Updates the item.

        @return: None
        """
        old_item = db._db.get_item(self._id)
        if self._pid is not None:
            parent = db._db.get_item(self._pid, get_lock=False)
        else:
            parent = None

        user = context.user
        user_role = permissions.resolve(old_item, user)

        if user_role > permissions.READER:
            # set security
            if user_role == permissions.COORDINATOR:
                # user is COORDINATOR
                if (self.inherit_roles != old_item.inherit_roles) or \
                        (not self.inherit_roles and
                         self.security != old_item.security):
                    self._apply_security(parent, False)
            else:
                # restore previous ACL
                self.security = old_item.security
                self.inherit_roles = old_item.inherit_roles

            self.modified_by = user.name
            self.modified = time.time()

            db._db.handle_update(self, old_item)
            db._db.put_item(self)
            if parent is not None:
                parent.modified = self.modified
            db._db.handle_post_update(self, old_item)
        else:
            raise exceptions.PermissionDenied(
                'The user does not have update permissions.')
