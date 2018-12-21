from porcupine import exceptions
from porcupine.datatypes import Relator1
from porcupine.core.services import db_connector
from .item import Item


class Shortcut(Item):
    """
    Shortcuts act as pointers to other objects.

    When adding a shortcut in a container the containment
    is checked against the target's content class and
    the shortcut's itself.
    When deleting an object that has shortcuts all its
    shortcuts are also deleted. Likewise, when restoring
    the object all of its shortcuts are also restored to
    their original location.
    It is valid to have shortcuts pointing to shortcuts.
    In order to resolve the terminal target object use the
    L{get_target} method.
    """
    target = Relator1(
        accepts=(Item, ),
        rel_attr='shortcuts',
        required=True,
    )

    @staticmethod
    def create(target):
        """Helper method for creating shortcuts of items.

        @param target: The target item
        @type target: L{Item}
        @return: L{Shortcut}
        """
        shortcut = Shortcut()
        shortcut.name = target.name
        shortcut.target = target.id
        return shortcut

    async def update(self):
        parent = await db_connector().get(self.parent_id)
        target = await self.get_target()
        if target.is_collection:
            containment_dt = parent.__class__.containers
        else:
            containment_dt = parent.__class__.items
        if not containment_dt.accepts_item(target):
            raise exceptions.ContainmentError(parent, 'children', target)
        return await super().update()

    async def get_target(self):
        """Returns the target item.

        @return: the target item or C{None} if the user
                 has no read permissions
        @rtype: L{Item} or NoneType
        """
        target = None
        if self.target:
            target = await self.target.item()
            while target and isinstance(target, Shortcut):
                target = await target.target.item()
        return target
