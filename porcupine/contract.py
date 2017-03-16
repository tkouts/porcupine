import asyncio
from functools import wraps
from sanic.request import Request
from porcupine.exceptions import UnprocessableEntity


def contract(accepts=None):
    def contract_decorator(func):
        @wraps(func)
        async def contract_wrapper(*args, **kwargs):
            # locate request
            try:
                request = [arg for arg in args if isinstance(arg, Request)][0]
            except IndexError:
                # TODO: raise error
                pass

            if accepts is not None:
                if not isinstance(request.json, accepts):
                    # TODO: add support for tuple accepts
                    raise UnprocessableEntity(
                        'Invalid payload, got {0} instead of {1}'.format(
                            type(request.json).__name__, accepts.__name__
                        ))

            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result
        return contract_wrapper
    return contract_decorator
