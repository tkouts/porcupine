from porcupine.schema import Container


class UserStorage(Container):
    pass


class UsersStorage(Container):
    containment = (UserStorage, )
