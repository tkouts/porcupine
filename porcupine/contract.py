import asyncio
from functools import wraps

from sanic.request import Request

from porcupine.exceptions import UnprocessableEntity, ServerError


def contract(accepts=None, predicate=None, optional=False):

    def contract_decorator(func):
        @wraps(func)
        async def contract_wrapper(*args, **kwargs):
            # locate request
            try:
                request = [arg for arg in args if isinstance(arg, Request)][0]
            except IndexError:
                raise ServerError(
                    'Missing request argument from contract handler')

            if not optional or request.json is not None:
                if accepts is not None:
                    if not isinstance(request.json, accepts):
                        # TODO: add support for tuple accepts
                        raise UnprocessableEntity(
                            'Invalid payload, got {0} instead of {1}.'.format(
                                type(request.json).__name__, accepts.__name__
                            ))

                if predicate is not None:
                    payload_invalid_message = predicate(request.json)
                    if payload_invalid_message:
                        raise UnprocessableEntity(payload_invalid_message)

            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result
        return contract_wrapper
    return contract_decorator


def is_new_item():

    def is_new(payload):
        if 'type' not in payload:
            return 'Missing item type key'

    return contract(accepts=dict, predicate=is_new)
