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

context = PContext()


class system_override:
    def __enter__(self):
        context.__system_update__ = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        context.__system_update__ = False


def with_context(co_routine):
    """
    Creates the security context
    :return: asyncio.Task
    """
    @wraps(co_routine)
    async def context_wrapper(*args, **kwargs):
        with Context():
            return await co_routine(*args, **kwargs)

    return context_wrapper
