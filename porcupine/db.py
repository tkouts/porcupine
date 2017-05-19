import asyncio
import copy
from functools import wraps
from typing import AsyncIterator, Optional

from sanic import Blueprint
from sanic.request import Request

from porcupine import context, exceptions
from porcupine.core.utils import system
from porcupine.hinting import TYPING

connector = None


async def get_item(item_id: str, quiet: bool=True) -> Optional[
        TYPING.ANY_ITEM_CO]:
    """
    Fetches an object from the database.
    If the user has no read permissions on the object
    or the item has been deleted then C{None} is returned.

    :param item_id: The object's ID or the object's full path.
    :type item_id: str

    :param quiet: Do not raise exceptions if the item
        does not exist or the user has no read permission.
    :type quiet: bool

    :rtype: L{GenericItem<porcupine.systemObjects.GenericItem>}
    """
    item = await connector.get(item_id, quiet=quiet)
    if item is not None:
        if context.system_override:
            return item
        visibility = await system.resolve_visibility(item, context.user)
        if visibility is not None:
            if visibility:
                return item
            elif not quiet:
                raise exceptions.Forbidden('Forbidden')
        elif not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))


async def get_multi(ids: TYPING.ID_LIST, return_none=False) -> AsyncIterator[
        Optional[TYPING.ANY_ITEM_CO]]:
    user = context.user
    is_override = context.system_override
    if ids:
        async for item in connector.get_multi(ids):
            if item is not None:
                if is_override:
                    yield item
                    continue
                visibility = await system.resolve_visibility(item, user)
                if visibility:
                    yield item
            elif return_none:
                yield None


def transactional(auto_commit=True):
    min_sleep_time = 0.010
    max_sleep_time = 0.288
    do_not_copy_types = (Request, Blueprint)

    def transactional_decorator(func):
        """
        This is the descriptor for making a function transactional.
        """
        @wraps(func)
        async def transactional_wrapper(*args, **kwargs):
            if context.txn is None:
                # top level function
                retries = 0
                sleep_time = min_sleep_time
                context.txn = txn = connector.get_transaction()
                max_retries = connector.txn_max_retries
                try:
                    while retries < max_retries:
                        # print('trying.... %d' % retries)
                        try:
                            args_copy = [
                                copy.deepcopy(arg)
                                if not isinstance(arg, do_not_copy_types)
                                else arg
                                for arg in args]
                            keyword_args_copy = {
                                name: copy.deepcopy(value)
                                if not isinstance(value, do_not_copy_types)
                                else value
                                for name, value in kwargs.items()
                            }
                            if retries > 0:
                                await asyncio.sleep(sleep_time)
                                sleep_time *= 2
                                if sleep_time > max_sleep_time:
                                    sleep_time = max_sleep_time + \
                                                 (retries * min_sleep_time)
                            val = await func(*args_copy,
                                             **keyword_args_copy)
                            if auto_commit:
                                await txn.commit()
                            else:
                                # abort if not committed
                                await txn.abort()
                            return val
                        except exceptions.DBDeadlockError:
                            await txn.abort()
                            retries += 1
                        except:
                            await txn.abort()
                            raise
                    # maximum retries exceeded
                    raise exceptions.DBDeadlockError
                finally:
                    context.txn = None
            else:
                # pass through
                return await func(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
