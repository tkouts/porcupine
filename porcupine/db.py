import asyncio
import copy
import time
from functools import wraps
from typing import AsyncIterator, Optional

from sanic import Blueprint
from sanic.request import Request

from porcupine import exceptions
from porcupine.hinting import TYPING
from porcupine.core.context import context
from porcupine.core.services import db_connector, get_service


async def _resolve_visibility(item: TYPING.ANY_ITEM_CO, user) -> Optional[int]:
    if item.__is_new__:
        return 1
    # check for stale / expired / deleted
    it = item
    connector = db_connector()
    while True:
        if not it.is_composite:
            # check expiration
            if it.expires_at is not None:
                if time.time() > it.expires_at:
                    # expired - remove from DB
                    await get_service('schema').remove_stale(it.id)
                    return None
            if it.is_deleted:
                return None
            if it.is_system:
                break
        if it.parent_id is None:
            break
        else:
            it = await connector.get(it.parent_id)
            if it is None:
                # stale - remove from DB
                await get_service('schema').remove_stale(it.id)
                return None

    return 1 if await item.can_read(user) else 0


async def get_item(item_id: str, quiet: bool = True) -> Optional[
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
    item = await db_connector().get(item_id, quiet=quiet)
    if item is not None:
        if context.system_override:
            return item
        visibility = await _resolve_visibility(item, context.user)
        if visibility is not None:
            if visibility:
                return item
            elif not quiet:
                raise exceptions.Forbidden('Forbidden')
        elif not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))


async def get_multi(ids: TYPING.ID_LIST, remove_stale=False) -> AsyncIterator[
        Optional[TYPING.ANY_ITEM_CO]]:
    user = context.user
    resolve_visibility = _resolve_visibility
    is_override = context.system_override
    if ids:
        connector = db_connector()
        async for item in connector.get_multi(ids):
            if item is not None:
                if is_override:
                    yield item
                    continue
                visibility = await resolve_visibility(item, user)
                if visibility:
                    yield item
            elif remove_stale:
                # TODO: remove stale
                pass


def transactional(auto_commit=True):
    min_sleep_time = 0.010
    max_sleep_time = 0.288
    do_not_copy_types = (Request, Blueprint, str, int, float, bool, tuple)

    def transactional_decorator(func):
        """
        This is the descriptor for making a function transactional.
        """
        @wraps(func)
        async def transactional_wrapper(*args, **kwargs):
            if context.txn is None:
                # top level co-routine
                connector = db_connector()
                retries = 0
                sleep_time = min_sleep_time
                max_retries = connector.txn_max_retries
                try:
                    while retries < max_retries:
                        # print('trying.... %d' % retries)
                        context.txn = txn = connector.get_transaction()
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
                        except Exception:
                            await txn.abort()
                            raise
                    # maximum retries exceeded
                    raise exceptions.DBDeadlockError(
                        'Maximum transaction retries exceeded')
                finally:
                    context.txn = None
            else:
                # pass through
                return await func(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
