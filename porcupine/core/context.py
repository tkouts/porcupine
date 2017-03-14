import asyncio
import pylru
from functools import wraps
from .aiolocals.local import Local, Context


class PContext(Local):
    @property
    def data(self):
        if not hasattr(self, '_data'):
            setattr(self, '_data', {})
        return self._data

    @property
    def txn(self):
        try:
            return self.__getattr__('txn')
        except AttributeError:
            return None

    @property
    def is_system_update(self):
        try:
            return self.__system_update__
        except AttributeError:
            return False

    @property
    def cache(self):
        try:
            return self.__cache__
        except AttributeError:
            lru_cache = pylru.lrucache(1000)
            self.__setattr__('__cache__', lru_cache)
            return lru_cache

context = PContext()


class system_override:
    def __enter__(self):
        context.__system_update__ = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        context.__system_update__ = False


def with_context(func):
    """
    Creates the security context
    :return: asyncio.Task
    """
    @wraps(func)
    async def context_wrapper(*args, **kwargs):
        with Context():
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result

    return context_wrapper


def context_cacheable(co_routine):
    """
    Caches the result of a coroutine in the context scope
    :return: asyncio.Task
    """
    @wraps(co_routine)
    async def cache_wrapper(*args):
        cache_key = (
            '{0}.{1}'.format(co_routine.__module__,
                             co_routine.__qualname__),
            args)
        print('CACHE KEY', cache_key)
        if cache_key in context.cache:
            print('CACHE HIT')
            return context.cache[cache_key]
        result = await co_routine(*args)
        context.cache[cache_key] = result
        return result

    return cache_wrapper
