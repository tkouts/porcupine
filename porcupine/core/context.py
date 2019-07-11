import contextvars
from functools import wraps
from lru import LRU

from porcupine.core.services import db_connector
from .log import porcupine_log

ctx_user = contextvars.ContextVar('user', default=None)
ctx_txn = contextvars.ContextVar('txn', default=None)
ctx_db_cache = contextvars.ContextVar('db_cache', default=None)
ctx_visibility_cache = contextvars.ContextVar('visibility_cache', default=None)
ctx_caches = contextvars.ContextVar('caches', default={})
ctx_sys = contextvars.ContextVar('__sys__', default=False)


class PContext:
    @property
    def system_override(self):
        return ctx_sys.get()

    @property
    def user(self):
        return ctx_user.get()

    @user.setter
    def user(self, user):
        ctx_user.set(user)

    @property
    def txn(self):
        return ctx_txn.get()

    @property
    def db_cache(self):
        return ctx_db_cache.get()

    @property
    def visibility_cache(self):
        return ctx_visibility_cache.get()

    @staticmethod
    def prepare():
        connector = db_connector()
        ctx_txn.set(None)
        ctx_db_cache.set(LRU(connector.cache_size))
        ctx_visibility_cache.set(LRU(100))


context = PContext()


class system_override:
    def __init__(self):
        self.token = None

    def __enter__(self):
        self.token = ctx_sys.set(True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        ctx_sys.reset(self.token)


def with_context(identity=None, debug=False):
    def decorator(task):
        """
        Creates the security context
        :return: asyncio.Task
        """
        @wraps(task)
        async def context_wrapper(*args, **kwargs):
            # with Context(locals=(context, )):
            connector = db_connector()
            context.prepare()
            user = identity
            if isinstance(user, str):
                user = await connector.get(user)
                if user is not None and user.is_deleted:
                    user = None
            ctx_user.set(user)
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
            caches = ctx_caches.get()
            if cache_name not in caches:
                caches[cache_name] = LRU(size)
            cache = caches[cache_name]
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
        self.token = None

    @property
    def original_user(self):
        if self.token:
            return self.token.old_value

    def __enter__(self):
        raise AttributeError('__enter__')

    def __exit__(self):
        ...

    async def __aenter__(self):
        if isinstance(self.user, str):
            connector = db_connector()
            should_switch = context.user is None \
                or context.user.id != self.user
            if should_switch:
                self.user = await connector.get(self.user, quiet=False)
        else:
            should_switch = (context.user and context.user.id) != \
                            (self.user and self.user.id)
        if should_switch:
            self.token = ctx_user.set(self.user)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.token:
            ctx_user.reset(self.token)
