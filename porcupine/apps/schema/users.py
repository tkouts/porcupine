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
    def __init__(self, storage=None):
        super().__init__(storage)
        if self.__is_new__:
            self.id = 'system'
            self.name = 'SYSTEM'
            self.description = 'System User'

    @staticmethod
    def is_admin():
        """
        System User is an administrative account.

        @return: C{True}
        """
        return True


class GenericUser(Item):
    """Generic User object

    :ivar full_name: The user's full name.
    :type full_name: L{String<porcupine.datatypes.String>}

    :cvar member_of: The list of groups that this user belongs to.
    :type member_of:
        L{MemberOf<org.innoscript.desktop.schema.properties.MemberOf>}

    :cvar policies: The list of policies assigned to this user.
    :type policies:
        L{Policies<org.innoscript.desktop.schema.properties.Policies>}
    """
    member_of = RelatorN(
        relates_to=('org.innoscript.desktop.schema.security.Group', ),
        rel_attr='members')
    policies = RelatorN(
        relates_to=('org.innoscript.desktop.schema.security.Policy', ),
        rel_attr='policyGranted')

    def is_member_of(self, group):
        """
        Checks if the user is member of the given group.

        @param group: the group object
        @type group: L{GenericGroup}

        @rtype: bool
        """
        return group.id in self.member_of

    def is_admin(self):
        """
        Checks if the user is direct member of the administrators group.

        @rtype: bool
        """
        return 'administrators' in self.member_of


class GuestUser(GenericUser):
    """
    Guest User
    ==========
    This user instance is assigned by the session manager
    to all newly created sessions.
    This is configurable. See the C{session_manager} section
    of C{porcupine.yaml}.
    """
    # def __init__(self, storage=None):
    #     super().__init__(storage=storage)
    #     self.id = 'guest'
    #     self.name = 'Guest'
    #     self.description = 'Guest User'


class User(GenericUser):
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
        if type(password) == str:
            password = password.encode('utf-8')
        md = hashlib.md5(password)
        hex_digest = md.hexdigest()
        return hex_digest == self.password


class UsersContainer(Container):
    """
    Users Folder
    ============
    This is the container of all users and groups.
    """
    containment = (GenericUser, )
