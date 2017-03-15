import time
import asyncio
import copy
from functools import wraps

from porcupine import context
from porcupine import exceptions
from porcupine.utils import permissions

connector = None


async def get_item(item_id, quiet=True):
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
        is_deleted = await item.is_deleted
        if not is_deleted:
            acl = await item.applied_acl
            access_level = await permissions.resolve(acl, context.user)
            if access_level != permissions.NO_ACCESS:
                return item
            elif not quiet:
                raise exceptions.Forbidden('Forbidden')
        elif not quiet:
            raise exceptions.NotFound(
                'The resource {0} does not exist'.format(item_id))


async def get_multi(ids):
    items = await connector.get_multi(ids)
    no_access = permissions.NO_ACCESS
    return [item for item in items
            if item is not None
            and not await item.is_deleted
            and await permissions.resolve(
                await item.applied_acl, context.user) != no_access]


def transactional(auto_commit=True):

    min_sleep_time = 0.010
    max_sleep_time = 0.288

    def transactional_decorator(function):
        """
        This is the descriptor for making a function transactional.
        """
        @wraps(function)
        async def transactional_wrapper(*args, **kwargs):
            if context.txn is None:
                # top level function
                now = time.time()
                retries = 0
                max_retries = connector.TransactionType.txn_max_retries
                sleep_time = min_sleep_time
                context.txn = connector.get_transaction()
                try:
                    while retries < max_retries:
                        # print('trying.... %d' % retries)
                        try:
                            args_copy = copy.deepcopy(args)
                            keyword_args_copy = copy.deepcopy(kwargs)
                            if retries > 0:
                                await asyncio.sleep(sleep_time)
                                sleep_time *= 2
                                if sleep_time > max_sleep_time:
                                    sleep_time = max_sleep_time + \
                                                 (retries * min_sleep_time)
                            val = await function(*args_copy,
                                                 **keyword_args_copy)
                            if auto_commit:
                                await context.txn.commit()
                            else:
                                # abort if not committed
                                await context.txn.abort()
                            print('TXN time', time.time() - now)
                            return val
                        except exceptions.DBDeadlockError:
                            await context.txn.abort()
                            retries += 1
                        except:
                            await context.txn.abort()
                            raise
                    # maximum retries exceeded
                    raise exceptions.DBDeadlockError
                finally:
                    context.txn = None
            else:
                # pass through
                return await function(*args, **kwargs)

        return transactional_wrapper

    return transactional_decorator
