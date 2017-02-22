# -*- coding: utf-8 -*-
"""
    Parts borrowed from werkzeug.local

    :copyright: (c) 2013 by the Werkzeug Team, see AUTHORS for more details.
    :license: BSD, see LICENSE for more details.
"""
import asyncio
from asyncio import futures
import contextlib
import io
import functools
import logging
import pprint
import threading
import traceback


def _get_ident():
    """
    Gets the current asyncio task id, or 0 if not found
    """
    try:
        loop = asyncio.get_event_loop()
    except AssertionError:
        """
        We`re in non-I/O thread at the moment.
        """
        return threading.current_thread().task_ident
    else:
        if loop.is_running():
            task = asyncio.Task.current_task()
            task_id = id(task)
            return task_id
        else:
            return 0


_contexts = {}
""" A map of task ids to Context objects """


log = logging.getLogger(__name__)


class Context:
    """
    Tracks a context, or set of locals for a given task.
    Should only be used as a context manager or via wrap_async
    """

    noisy = False

    # noinspection PyShadowingBuiltins
    def __init__(self, ident=None, locals=None, parent=None):
        """
        :param ident: The function to use to find a local state identifier
        :param locals: A list of Local instances to manage
        :param parent: The parent context
        """

        if ident is None:
            ident = _get_ident()
        if locals is None:
            locals = []  # pragma: no cover
        self.ident = ident
        self.locals = locals

        if parent:
            self.parent = parent
            for l in parent.locals:
                l.__copy_from_parent__(self.ident, parent.ident)
        else:
            self.parent = None

    def cleanup(self):
        if self.noisy:
            _io = io.StringIO()
            traceback.print_stack(file=_io)
            _io.seek(0)
            log.debug("aiolocals.Context.cleanup: Leaving context %r", self)
            log.debug("aiolocals.Context.cleanup: Trace: %s", _io.read())
        _contexts.pop(self.ident)
        for local in self.locals:
            local.__release_local__(self.ident)

    def __enter__(self):
        if self.noisy:
            _io = io.StringIO()
            traceback.print_stack(file=_io)
            _io.seek(0)
            log.debug("aiolocals.Context.__enter__: Entering context %r", self)
            log.debug("aiolocals.Context.__enter__: Trace: %s", _io.read())
        _contexts[self.ident] = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def __repr__(self):
        return "<aiolocals.Context ident=%s locals=%r parent=%r>" \
               % (self.ident, self.locals, self.parent)


def _spawn_context(child_ident, parent_ident=None):
    parent = None if not parent_ident else _contexts.get(parent_ident)
    ctx = Context(child_ident, locals=parent.locals if parent else [],
                  parent=parent)
    ctx.__enter__()
    return ctx


def wrap_async(coro, **kwargs):
    """
    Wraps a coroutine with a Task that runs in the background.
    It ensures any context information is transferred
    to the new task

    :param coro: The coroutine to wrap as an asynchronous task
    """
    parent_ident = _get_ident()
    t = asyncio.async(coro, **kwargs)
    """ :type: asyncio.Task """

    parent_ctx = _contexts.get(parent_ident, None)
    if parent_ctx:
        child_ident = id(t)
        child_ctx = _spawn_context(child_ident=child_ident,
                                   parent_ident=parent_ident)

        def cb(_):
            child_ctx.cleanup()
        t.add_done_callback(cb)

    return t


class _TaskList(asyncio.Future):

    def __init__(self, children, *, loop=None):
        super().__init__(loop=loop)
        self._children = children

    def cancel(self):
        if self.done():
            return False
        for child in self._children:
            child.cancel()
        return True


def wrap_gather(*tasks, loop=None, return_exceptions=False):
    """
    Return a task aggregating results from the given tasks.
    Any context information for each task if transferred to the new task

    :param tasks: Coroutines or tasks to perform
    :param loop: Event loop
    :param return_exceptions: Boolean indicating whether exceptions are
                              returned
    """
    children = [wrap_async(task, loop=loop) for task in tasks]
    n = len(children)
    if n == 0:
        outer = asyncio.Future(loop=loop)
        outer.set_result([])
        return outer
    outer = wrap_async(_TaskList(children, loop=loop))
    nfinished = 0
    results = [None] * n

    def _done_callback(i, task):
        nonlocal nfinished
        if outer._state != futures._PENDING:
            if task._exception is not None:
                # Mark exception retrieved.
                task.exception()  # pragma: no cover
            return
        if task._state == futures._CANCELLED:
            res = futures.CancelledError()
            if not return_exceptions:
                outer.set_exception(res)
                return
        elif task._exception is not None:
            res = task.exception()  # Mark exception retrieved.
            if not return_exceptions:
                outer.set_exception(res)
                return
        else:
            res = task._result
        results[i] = res
        nfinished += 1
        if nfinished == n:
            outer.set_result(results)

    for i, task in enumerate(children):
        task.add_done_callback(functools.partial(_done_callback, i))
    return outer


class Local(object):
    """
    An object that stores different state for different task contexts.
    Meant to be used with Context.

    To use within a Context, simply set arbitrary attributes on this object
    and they will be tracked automatically.
    """

    noisy = False

    __slots__ = ('__storage__', '__ident_func__')

    def __init__(self):
        object.__setattr__(self, '__storage__', {})
        object.__setattr__(self, '__ident_func__', _get_ident)

    def __release_local__(self, ident=None):
        if not ident:
            ident = self.__ident_func__()
        if self.noisy:
            log.debug("aiolocals.Local: Cleaning local ident %s, storage=%s",
                      ident,  pprint.pformat(self.__storage__))
        self.__storage__.pop(ident, None)
        if self.noisy:
            log.debug("aiolocals.Local: Released local ident %s, storage=%s",
                      ident, pprint.pformat(self.__storage__))

    def __copy_from_parent__(self, child_ident, parent_ident):
        self.__storage__[child_ident] = self.__storage__[parent_ident]

    def __getattr__(self, name):
        try:
            ident = self.__ident_func__()
            if ident not in _contexts:
                raise ValueError("Can't access a local outside a context")
            return self.__storage__[ident][name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        ident = self.__ident_func__()
        if ident not in _contexts:
            raise ValueError("Can't set a local outside a context")
        storage = self.__storage__
        try:
            storage[ident][name] = value
        except KeyError:
            storage[ident] = {name: value}

    def __delattr__(self, name):
        try:
            ident = self.__ident_func__()
            if ident not in _contexts:
                raise ValueError("Can't set a local outside a context")
            del self.__storage__[ident][name]
        except KeyError:
            raise AttributeError(name)


@contextlib.contextmanager
def preserve_context_in_threads():
    """
    Usage example:

    .. code::

        some_local = Local()

        def some_calculations():
            time.sleep(1)
            print("BTW, spam is %s" % some_local.spam)

        @asyncio.coroutine
        def main_multithreaded():
            with Context(locals=(some_local,)):
                some_local.spam = 'ham'
                with preserve_context_in_threads():
                    yield from asyncio.get_event_loop().run_in_executor(
                        ThreadPoolExecutor(1), some_calculations)
    """

    __old_thread_init__ = threading.Thread.__init__

    def __init__(self, *args, **kwargs):
        self.task_ident = _get_ident()
        __old_thread_init__(self, *args, **kwargs)

    threading.Thread.__init__ = __init__

    yield
    threading.Thread.__init__ = __old_thread_init__


__all__ = ["Local", "wrap_async", "wrap_gather", "Context",
           "preserve_context_in_threads"]
