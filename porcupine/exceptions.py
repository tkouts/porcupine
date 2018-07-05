from sanic.exceptions import SanicException, InvalidUsage, \
    NotFound, ServerError, ServiceUnavailable


InvalidUsage = InvalidUsage
NotFound = NotFound
ServiceUnavailable = ServiceUnavailable


class SchemaError(Exception):
    pass


class Conflict(SanicException):
    status_code = 409


class DBDeadlockError(ServerError):
    pass


class DBAlreadyExists(Conflict):
    pass


class Forbidden(SanicException):
    status_code = 403


class MethodNotAllowed(SanicException):
    status_code = 405


class UnprocessableEntity(SanicException):
    status_code = 422


class ContainmentError(TypeError):
    def __init__(self, target_item, attribute, source_item):
        super().__init__(
            "Attribute '{0}' of '{1}' does not accept objects of '{2}'".format(
                attribute, target_item.__class__.__name__,
                source_item.content_class))


AttributeSetError = (AttributeError, ValueError, TypeError)
