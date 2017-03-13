from sanic.exceptions import SanicException, InvalidUsage


class Conflict(SanicException):
    status_code = 409


class DBDeadlockError(Conflict):
    pass


class DBAlreadyExists(Conflict):
    pass


class Forbidden(SanicException):
    status_code = 403


class ContainmentError(InvalidUsage):
    def __init__(self, target_item, attribute, source_item):
        super().__init__(
            'Attribute {0} of {1} does not accept objects of {2}'.format(
                attribute, target_item.name, source_item.content_class))
