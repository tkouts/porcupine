from porcupine.datatypes import Relator1
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
        relates_to=('porcupine.schema.Item', ),
        rel_attr='shortcuts',
        required=True,
    )

    @staticmethod
    def create(target):
        """Helper method for creating shortcuts of items.

        @param target: The id of the item or the item object itself
        @type target: str OR L{Item}
        @return: L{Shortcut}
        """
        if isinstance(target, str):
            target = db._db.get_item(target)
        shortcut = Shortcut()
        shortcut.name = target.name
        shortcut.target = target.id
        return shortcut

    # @db.requires_transactional_context
    def append_to(self, parent):
        if isinstance(parent, str):
            parent = db._db.get_item(parent)

        content_class = self.get_target_contentclass()
        if content_class not in parent.containment:
            raise exceptions.ContainmentError(
                'The target container does not accept '
                'objects of type\n"%s".' % content_class)
        else:
            return super(Shortcut, self).append_to(parent)

    # @db.requires_transactional_context
    def copy_to(self, target):
        if isinstance(target, str):
            target = db._db.get_item(target)

        content_class = self.get_target_contentclass()
        if content_class not in target.containment:
            raise exceptions.ContainmentError(
                'The target container does not accept '
                'objects of type\n"%s".' % content_class)
        else:
            return super(Shortcut, self).copy_to(target)

    # @db.requires_transactional_context
    def move_to(self, target, inherit_roles=False):
        if isinstance(target, str):
            target = db._db.get_item(target)

        content_class = self.get_target_contentclass()
        if content_class not in target.containment:
            raise exceptions.ContainmentError(
                'The target container does not accept '
                'objects of type\n"%s".' % content_class)
        else:
            return super(Shortcut, self).move_to(target, inherit_roles)

    # @db.requires_transactional_context
    def update(self):
        parent = db._db.get_item(self._pid)
        content_class = self.get_target_contentclass()
        if content_class not in parent.containment:
            raise exceptions.ContainmentError(
                'The parent container does not accept '
                'objects of type\n"%s".' % content_class)
        else:
            return super(Shortcut, self).update()

    def get_target(self, get_lock=True):
        """Returns the target item.

        @return: the target item or C{None} if the user
                 has no read permissions
        @rtype: L{Item} or NoneType
        """
        target = None
        if self.target:
            target = self.target.get_item(get_lock=get_lock)
            while target and isinstance(target, Shortcut):
                target = target.target.get_item(get_lock=get_lock)
        return target

    def get_target_contentclass(self):
        """Returns the content class of the target item.

        @return: the fully qualified name of the target's
                 content class
        @rtype: str
        """
        if self.target:
            target = db._db.get_item(self.target, get_lock=False)
            while isinstance(target, Shortcut):
                target = db._db.get_item(target.target, get_lock=False)
            return target.contentclass
