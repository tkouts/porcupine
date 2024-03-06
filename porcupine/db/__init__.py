import asyncio
import copy
from functools import wraps
from typing import Optional

from sanic import Blueprint
from sanic.request import Request
from aiostream import pipe

from porcupine import exceptions
from porcupine.hinting import TYPING
from porcupine.core.context import ctx_db, context
from porcupine.core.services import db_connector
from porcupine.core.stream.streamer import EmptyStreamer, BaseStreamer


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
    db = ctx_db.get()
    item = await db.get(item_id, quiet=quiet)
    if item is not None:
        can_read = await item.can_read(context.user)
        if can_read:
            return item
        elif not quiet:
            raise exceptions.Forbidden(
                f'Access to resource {item_id} is forbidden.'
            )


def get_multi(ids: TYPING.ID_LIST, quiet: bool = True) -> BaseStreamer:
    user = context.user
    db = ctx_db.get()

    async def _check_read_permission(item):
        can_read = await item.can_read(user)
        if can_read:
            return True
        elif not quiet:
            raise exceptions.Forbidden(
                f'Access to resource {item.id} is forbidden.'
            )
        return False

    if ids:
        streamer = (
            BaseStreamer(db.get_multi(ids, quiet))
            | pipe.filter(_check_read_permission)
        )
        return streamer
    return EmptyStreamer()


def transactional(auto_commit=True):
    min_sleep_time = 0.020
    max_sleep_time = 0.320
    do_not_copy_types = (Request, Blueprint, str, int, float, bool, tuple)

    def transactional_decorator(func):
        """
        This is the descriptor for making a function transactional.
        """
        @wraps(func)
        async def transactional_wrapper(*args, **kwargs):
            db = ctx_db.get()
            if db.txn is None:
                # top level co-routine
                connector = db_connector()
                connector.active_txns += 1
                retries = 0
                sleep_time = min_sleep_time
                max_retries = connector.txn_max_retries
                # try:
                while retries < max_retries:
                    # print('trying.... %d' % retries)
                    txn = db.get_transaction()
                    try:
                        args_copy = [
                            copy.deepcopy(arg)
                            if not isinstance(arg, do_not_copy_types)
                            else arg
                            for arg in args
                        ]
                        keyword_args_copy = {
                            name: copy.deepcopy(value)
                            if not isinstance(value, do_not_copy_types)
                            else value
                            for name, value in kwargs.items()
                        }
                        if not auto_commit:
                            keyword_args_copy['txn'] = txn
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
                    finally:
                        connector.active_txns -= 1
                # maximum retries exceeded
                raise exceptions.DBDeadlockError(
                    'Maximum number of transaction retries exceeded.'
                )
            else:
                # pass through
                return await func(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
