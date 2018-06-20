from functools import wraps
from lru import LRU
from .log import porcupine_log
from .aiolocals.local import Local, Context

connector = None


class PContext(Local):
    @property
    def system_override(self):
        return self.__sys__

    def prepare(self):
        global connector
        if connector is None:
            from porcupine.db import connector
        self.__setattr__('__sys__', False)
        self.__setattr__('caches', {})
        self.__setattr__('db_cache', LRU(connector.cache_size))
        self.__setattr__('txn', None)


context = PContext()


class system_override:
    def __init__(self):
        self.override = context.__sys__

    def __enter__(self):
        context.__sys__ = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        context.__sys__ = self.override


def with_context(identity=None, debug=False):
    def decorator(task):
        """
        Creates the security context
        :return: asyncio.Task
        """
        @wraps(task)
        async def context_wrapper(*args, **kwargs):
            with Context(locals=(context, )):
                context.prepare()
                user = identity
                if isinstance(user, str):
                    user = await connector.get(user)
                    if user is not None and user.is_deleted:
                        user = None
                context.user = user
                try:
                    return await task(*args, **kwargs)
                finally:
                    if debug:
                        size = len(context.db_cache)
                        hits, misses = context.db_cache.get_stats()
                        porcupine_log.debug(
                            'Cache Size: {0} Hits: {1} Misses: {2}'
                            .format(size, hits, misses))

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


class context_user:
    def __init__(self, user):
        self.user = user
        self.original_user = None
        self.user_switched = False

    def __enter__(self):
        raise AttributeError('__enter__')

    def __exit__(self):
        ...

    async def __aenter__(self):
        if isinstance(self.user, str):
            should_switch = context.user is None \
                            or context.user.id != self.user
            if should_switch:
                self.user = await connector.get(self.user, quiet=False)
        else:
            should_switch = (context.user and context.user.id) != \
                            (self.user and self.user.id)
        if should_switch:
            self.original_user = context.user
            context.user = self.user
            self.user_switched = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.user_switched:
            context.user = self.original_user
