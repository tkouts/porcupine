from sanic.exceptions import SanicException, InvalidUsage, \
    NotFound, ServerError, ServiceUnavailable


InvalidUsage = InvalidUsage
NotFound = NotFound
ServiceUnavailable = ServiceUnavailable


class SchemaError(Exception):
    pass


class Conflict(SanicException):
    status_code = 409


class DBDeadlockError(Conflict):
    pass


class OqlSyntaxError(ServerError):
    pass


class OqlError(ServerError):
    pass


class DBError(Exception):
    def __init__(self, message=None, mutation=None):
        super().__init__(message)
        self.mutation = mutation

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(cause={self.__cause__} "
            f"mutation={self.mutation})"
        )

    def __str__(self):
        return self.__repr__()


class DBAlreadyExists(DBError, Conflict):
    pass


class Forbidden(SanicException):
    status_code = 403


class MethodNotAllowed(SanicException):
    status_code = 405


class UnprocessableEntity(SanicException):
    status_code = 422


class ContainmentError(InvalidUsage, TypeError):
    def __init__(self, target_item, attribute, source_item):
        super().__init__(
            f"Attribute '{attribute}' of type "
            f"'{target_item.__class__.__name__}' "
            f"does not accept objects of type '{source_item.content_class}'."
        )


AttributeSetError = (AttributeError, ValueError, TypeError)
