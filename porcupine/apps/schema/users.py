import hashlib

from porcupine import db, permissions, context_user
from porcupine.schema import Container
from porcupine.schema import Item
from porcupine.datatypes import String, RelatorN, Password, Reference1, \
    Dictionary, Email

from .userstorage import UserStorage


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
            self.__storage__.name = 'SYSTEM'
            self.__storage__.description = 'System User'

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
        accepts=('Group', ),
        rel_attr='members'
    )
    policies = RelatorN(
        accepts=(
            # 'org.innoscript.desktop.schema.security.Policy',
        ),
        rel_attr='granted_to'
    )

    async def is_member_of(self, group) -> bool:
        """
        Checks if the user is member of the given group.

        @param group: the group object
        @type group: L{GenericGroup}

        @rtype: bool
        """
        group_ids = [group_id async for group_id in self.member_of]
        return group.id in group_ids

    async def is_admin(self) -> bool:
        """
        Checks if the user is direct member of the administrators group.

        @rtype: bool
        """
        group_ids = [group_id async for group_id in self.member_of]
        return 'administrators' in group_ids


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
    email = Email()
    password = Password()
    settings = Dictionary()
    personal_folder = Reference1(accepts=(UserStorage, ),
                                 cascade_delete=True,
                                 required=True)

    def authenticate(self, password):
        """Checks if the given string matches the
        user's password.

        @param password: The string to check against.
        @type password: str

        @rtype: bool
        """
        if password == self.password:
            return True
        hex_digest = hashlib.sha3_256(password.encode('utf-8')).hexdigest()
        return hex_digest == self.password

    async def on_create(self):
        # create user's storage
        async with context_user('system'):
            storage_container = await db.get_item('ustorage')
            user_storage = UserStorage()
            user_storage.name = self.name
            # set acl
            await user_storage.acl.reset({
                self.id: permissions.CONTENT_CO
            })
            await user_storage.append_to(storage_container)
            self.personal_folder = user_storage.id
        return user_storage

    async def on_change(self):
        if self.name != self.get_snapshot_of('name'):
            async with context_user('system'):
                user_storage = await self.personal_folder.item()
                user_storage.name = self.name
                await user_storage.update()


class UsersContainer(Container):
    """
    Users Folder
    ============
    This is the container of all users and groups.
    """
    containment = (Membership, )
