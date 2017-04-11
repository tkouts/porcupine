import hashlib
from porcupine.schema import Container
from porcupine.schema import Item
from porcupine.datatypes import String, RelatorN, Password, Reference1, \
    Dictionary


class SystemUser(Item):
    """
    System User
    ===========
    System User is a special user.
    Use this identity for performing actions not initiated by users.
    This user has no security restrictions.
    """
    def __init__(self, dict_storage=None):
        super().__init__(dict_storage)
        if self.__is_new__:
            # direct assignment of id. don't try this at home!
            self.__storage__.id = 'system'
            self.name = 'SYSTEM'
            self.description = 'System User'

    @staticmethod
    async def is_admin():
        """
        System User is an administrative account.

        @return: C{True}
        """
        return True


class Membership(Item):
    """Generic Membership object

    :cvar member_of: The list of groups that the membership belongs to.
    :type member_of:
        L{MemberOf<org.innoscript.desktop.schema.properties.MemberOf>}

    :cvar policies: The list of policies assigned to this membership.
    :type policies:
        L{Policies<org.innoscript.desktop.schema.properties.Policies>}
    """
    member_of = RelatorN(
        accepts=('porcupine.apps.schema.groups.Group', ),
        rel_attr='members'
    )
    policies = RelatorN(
        accepts=(
            # 'org.innoscript.desktop.schema.security.Policy',
        ),
        rel_attr='granted_to'
    )

    async def is_member_of(self, group):
        """
        Checks if the user is member of the given group.

        @param group: the group object
        @type group: L{GenericGroup}

        @rtype: bool
        """
        return group.id in await self.member_of.get()

    async def is_admin(self):
        """
        Checks if the user is direct member of the administrators group.

        @rtype: bool
        """
        return 'administrators' in await self.member_of.get()


class User(Membership):
    """Porcupine User object

    @ivar password: The user's password.
    @type password: L{Password<porcupine.dt.Password>}

    @ivar email: The user's email.
    @type email: L{String<porcupine.dt.String>}

    @ivar settings: User specific preferences.
    @type settings: L{Dictionary<porcupine.dt.Dictionary>}
    """
    first_name = String()
    last_name = String()
    email = String()
    password = Password(required=True)
    settings = Dictionary()
    personal_folder = Reference1()
    # event_handlers = GenericUser.event_handlers +
    # [handlers.PersonalFolderHandler]

    def authenticate(self, password):
        """Checks if the given string matches the
        user's password.

        @param password: The string to check against.
        @type password: str

        @rtype: bool
        """
        hex_digest = hashlib.sha3_256(
            password.encode('utf-8')).hexdigest()
        return hex_digest == self.password


class UsersContainer(Container):
    """
    Users Folder
    ============
    This is the container of all users and groups.
    """
    containment = (Membership, )
