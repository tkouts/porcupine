import asyncio
import copy
import time
from functools import wraps
from typing import AsyncIterator, Optional

from sanic import Blueprint
from sanic.request import Request

from porcupine import exceptions
from porcupine.hinting import TYPING
from porcupine.core.context import context, ctx_txn
from porcupine.core.services import db_connector, get_service


async def _resolve_visibility(item: TYPING.ANY_ITEM_CO, user) -> Optional[int]:
    if item.__is_new__:
        return 1

    # check for stale / expired / deleted
    cache_key = f'{item.parent_id}|{user.id if user else ""}'

    if cache_key in context.visibility_cache:
        visibility = context.visibility_cache[cache_key]
        if visibility is None:
            return None
        if not item.acl.is_set():
            return visibility

    it = item
    visibility = 0
    connector = db_connector()
    ttl_supported = connector.supports_ttl
    _get = connector.get
    while True:
        if not it.is_composite:
            # check expiration
            if not ttl_supported and it.expires_at is not None:
                if time.time() > it.expires_at:
                    # expired - remove from DB
                    await get_service('schema').remove_stale(it.id)
                    visibility = None
                    break
            if it.is_deleted:
                visibility = None
                break
            if it.is_system:
                break
        if it.parent_id is None:
            break
        else:
            it = await _get(it.parent_id)
            if it is None:
                # stale - remove from DB
                await get_service('schema').remove_stale(it.id)
                visibility = None
                break

    if visibility is None:
        if it != item:
            # add to perms cache
            context.visibility_cache[cache_key] = visibility
    else:
        visibility = 1 if await item.can_read(user) else 0
        if not item.acl.is_set():
            context.visibility_cache[cache_key] = visibility

    return visibility


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
    connector = db_connector()
    item = await connector.get(item_id, quiet=quiet)
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


async def get_multi(ids: TYPING.ID_LIST, _collection=None) -> AsyncIterator[
        Optional[TYPING.ANY_ITEM_CO]]:
    if ids:
        user = context.user
        resolve_visibility = _resolve_visibility
        is_override = context.system_override
        stale = []
        connector = db_connector()
        async for item_id, item in connector.get_multi(ids):
            if item is not None:
                if is_override:
                    yield item
                else:
                    visibility = await resolve_visibility(item, user)
                    if visibility:
                        yield item
            elif _collection is not None:
                stale.append(item_id)

        if stale:
            await get_service('schema').clean_collection(_collection['key'],
                                                         stale,
                                                         _collection['ttl'])


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
            if ctx_txn.get() is None:
                # top level co-routine
                connector = db_connector()
                retries = 0
                sleep_time = min_sleep_time
                max_retries = connector.txn_max_retries
                try:
                    while retries < max_retries:
                        # print('trying.... %d' % retries)
                        # context.txn = txn = connector.get_transaction()
                        txn = connector.get_transaction()
                        ctx_txn.set(txn)
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
                            elif not txn.committed:
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
                    ctx_txn.set(None)
            else:
                # pass through
                return await func(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
