import asyncio
import copy
from functools import wraps
from typing import Optional, AsyncIterable

from sanic import Blueprint
from sanic.request import Request
from aiostream import stream, pipe

from porcupine import exceptions
from porcupine.hinting import TYPING
from porcupine.core.context import ctx_txn
from porcupine.core.services import db_connector
from porcupine.core.utils.db import resolve_visibility


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
        visibility = await resolve_visibility(item)
        if visibility is not None:
            if visibility:
                return item
            elif not quiet:
                raise exceptions.Forbidden(
                    f'Access to resource {item_id} is forbidden'
                )
        elif not quiet:
            raise exceptions.NotFound(f'The resource {item_id} does not exist')


async def get_multi(ids: TYPING.ID_LIST) -> AsyncIterable[TYPING.ANY_ITEM_CO]:
    connector = db_connector()
    streamer = (
        stream.iterate(connector.get_multi(ids))
        | pipe.filter(resolve_visibility)
    )
    async with streamer.stream() as items:
        async for item in items:
            yield item


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
                # try:
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
                # finally:
                #     ctx_txn.set(None)
            else:
                # pass through
                return await func(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
