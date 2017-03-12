import datetime

from porcupine import db, context
from porcupine.core.context import system_override
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

                    # db._db.handle_update(self, old_item)
                    # db._db.put_item(self)
                    context.txn.update(self)
                    if parent is not None:
                        parent.modified = self.modified
                        context.txn.update(parent)
                    # db._db.handle_post_update(self, old_item)
            else:
                raise exceptions.PermissionDenied(
                    'The user does not have update permissions.')
