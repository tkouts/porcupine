from contextvars import ContextVar
from functools import wraps
from lru import LRU

from porcupine.core.services import db_connector
from .log import porcupine_log

ctx_user = ContextVar('user', default=None)
ctx_sys = ContextVar('__sys__', default=False)
ctx_visibility_cache = ContextVar('visibility_cache')
ctx_membership_cache = ContextVar('membership_cache')
ctx_access_map = ContextVar('access_map')
ctx_db = ContextVar('db')


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
    def db(self):
        return ctx_db.get()

    @property
    def access_map(self):
        return ctx_access_map.get()

    @property
    def visibility_cache(self):
        return ctx_visibility_cache.get()

    @staticmethod
    def prepare():
        ctx_visibility_cache.set(LRU(128))
        ctx_membership_cache.set(LRU(16))
        ctx_access_map.set({})


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
            connector = db_connector()
            context.prepare()
            async with connector.acquire() as db_connection:
                ctx_db.set(db_connection)
                user = identity
                if isinstance(user, str):
                    user = await db_connection.get(user)
                ctx_user.set(user)
                try:
                    return await task(*args, **kwargs)
                finally:
                    if debug:
                        size = len(db_connection.cache)
                        hits, misses = db_connection.cache.get_stats()
                        porcupine_log.debug(
                            f'Cache Size: {size} Hits: {hits} Misses: {misses}'
                        )
        return context_wrapper
    return decorator


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
            db = ctx_db.get()
            should_switch = (
                context.user is None
                or context.user.id != self.user
            )
            if should_switch:
                self.user = await db.get(self.user, quiet=False)
        else:
            should_switch = (
                (context.user and context.user.id)
                != (self.user and self.user.id)
            )
        if should_switch:
            self.token = ctx_user.set(self.user)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.token:
            ctx_user.reset(self.token)
