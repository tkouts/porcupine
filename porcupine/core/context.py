import asyncio
from lru import LRU
from functools import wraps
from .aiolocals.local import Local, Context


class PContext(Local):
    @property
    def txn(self):
        try:
            return self.__getattr__('txn')
        except AttributeError:
            return None

    @property
    def is_system_update(self):
        try:
            return self.__getattr__('__sys__')
        except AttributeError:
            return False

    @property
    def caches(self):
        try:
            return self.__caches__
        except AttributeError:
            caches = {}
            self.__setattr__('__caches__', caches)
            return caches

    def reset(self):
        # clear caches
        # for cache in self.caches.values():
        #     cache.clear()
        setattr(self, '__caches__', {})
        setattr(self, 'user', None)
        setattr(self, 'txn', None)


context = PContext()


class system_override:
    def __enter__(self):
        context.__sys__ = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        context.__sys__ = False


def with_context(identity=None):
    from porcupine import db

    def decorator(func):
        """
        Creates the security context
        :return: asyncio.Task
        """
        @wraps(func)
        async def context_wrapper(*args, **kwargs):
            with Context():
                user = identity
                if identity is not None and isinstance(identity, str):
                    user = await db.connector.get(identity)
                context.user = user
                try:
                    result = func(*args, **kwargs)
                    if asyncio.iscoroutine(result):
                        return await result
                    return result
                finally:
                    context.reset()
        return context_wrapper
    return decorator


def context_cacheable(size=100):
    """
    Caches the result of a coroutine in the context scope
    :return: asyncio.Task
    """
    def cache_decorator(co_routine):
        @wraps(co_routine)
        async def cache_wrapper(*args):
            # initialize cache
            cache_name = '{0}.{1}'.format(co_routine.__module__,
                                          co_routine.__qualname__)
            if cache_name not in context.caches:
                context.caches[cache_name] = LRU(size)
            cache = context.caches[cache_name]
            cache_key = args
            # print('CACHE KEY', cache_key)
            if cache_key in cache:
                # print('CACHE HIT')
                return cache[cache_key]
            result = await co_routine(*args)
            cache[cache_key] = result
            return result
        return cache_wrapper
    return cache_decorator
