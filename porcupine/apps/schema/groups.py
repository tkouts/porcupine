from porcupine.schema import Item
from porcupine.datatypes import RelatorN
from .users import Membership


class Group(Membership):
    """Security Group

    @ivar members: The group's members.
    @type members: L{Members<org.innoscript.desktop.schema.properties.Members>}
    """
    members = RelatorN(
        accepts=(Membership, ),
        rel_attr='member_of'
    )

    async def has_member(self, user):
        """
        Checks if a user is direct member of this group.

        @param user: the user object
        @type user: L{GenericUser}

        @rtype: bool
        """
        return user.id in await self.members.get()


class EveryoneGroup(Item):
    """Everyone Pseudo-Group"""

    @staticmethod
    async def has_member(user):
        """
        This method always returns C{True}.

        @param user: the user object
        @type user: L{GenericUser}

        @return: C{True}
        @rtype: bool
        """
        if isinstance(user, Membership):
            return True
        return False


class AuthUsersGroup(Item):
    """Authenticated Users Pseudo-Group"""

    @staticmethod
    async def has_member(user):
        """
        This method returns C{True} only if the user object has
        an attribute named C{password} else it returns C{False}.

        @param user: the user object
        @type user: L{GenericUser}

        @rtype: bool
        """
        return hasattr(user, 'authenticate')
