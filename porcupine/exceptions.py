class DBConnectionError(Exception):
    pass


class DBDeadlockError(Exception):
    pass


class DBAlreadyExists(Exception):
    pass
