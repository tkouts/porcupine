import io
import random
import hashlib
import functools

from porcupine import db
from porcupine.core.context import context_cacheable


VALID_ID_CHARS = [
    chr(x) for x in
    list(range(ord('a'), ord('z'))) +
    list(range(ord('A'), ord('Z'))) +
    list(range(ord('0'), ord('9')))
]


def generate_oid(length=8):
    """
    Generates a random Object ID string.

    @rtype: str
    """
    return ''.join(random.sample(VALID_ID_CHARS, length))


@functools.lru_cache()
def get_rto_by_name(name):
    """
    This function returns a runtime object by name.

    For example::

        get_rto_by_name('org.innoscript.desktop.schema.common.Folder')()

    instantiates a new I{Folder} object.

    @rtype: type
    """
    modules = name.split('.')
    if len(modules) == 1:
        module = modules[0]
        attribute = None
    else:
        module = '.'.join(modules[:-1])
        attribute = modules[-1]
    is_second_level = False

    try:
        mod = __import__(module, globals(), locals(),
                         [attribute] if attribute else [])
    except ImportError:
        is_second_level = True
        mod = __import__('.'.join(modules[:-2]), globals(), locals(),
                         [modules[-2]])

    if attribute:
        if is_second_level:
            return getattr(getattr(mod, modules[-2]), attribute)
        return getattr(mod, attribute)
    else:
        return mod


def hash_series(*args, using='md5'):
    with io.BytesIO() as bt:
        for arg in args:
            if isinstance(arg, str):
                arg = arg.encode('utf-8')
            bt.write(arg)
        md5_hash = getattr(hashlib, using)(bt.getvalue())
    return md5_hash


@context_cacheable(1000)
async def get_item_state(item_id):
    return await db.connector.get_partial(
        item_id, 'p_id', 'acl', 'deleted', 'sys',
        snapshot=True)


@context_cacheable(100)
async def resolve_deleted(item_id):
    state = await get_item_state(item_id)
    while not state['deleted'] and state['p_id'] is not None:
        if state['sys']:
            break
        state = await get_item_state(state['p_id'])
    return state['deleted']


@context_cacheable(100)
async def resolve_acl(object_id):
    state = await get_item_state(object_id)
    while state['acl'] is None and state['p_id'] is not None:
        state = await get_item_state(state['p_id'])
    return state['acl']
