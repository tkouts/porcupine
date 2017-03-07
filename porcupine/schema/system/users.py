from ..item import Item


class SystemUser(Item):
    """
    System User
    ===========
    System User is a special user.
    Use this identity for performing actions not initiated by users.
    This user has no security restrictions.
    """
    def __init__(self):
        super().__init__()
        self.id = 'system'
        self.name = 'SYSTEM'
        self.description = 'System User'

    @staticmethod
    def is_admin():
        """
        System User is an administative account.

        @return: C{True}
        """
        return True
