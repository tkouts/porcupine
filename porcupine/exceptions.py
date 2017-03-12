from sanic.exceptions import SanicException


class Conflict(SanicException):
    status_code = 409


class DBDeadlockError(Conflict):
    pass


class DBAlreadyExists(Conflict):
    pass


class Forbidden(SanicException):
    status_code = 403
