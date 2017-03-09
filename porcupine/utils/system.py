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


@context_cacheable
async def get_item_state(item_id):
    return await db.connector.get_partial(
        item_id, 'p_id', 'acl', 'deleted', snapshot=True)


async def resolve_deleted(item):
    if item.deleted or item.p_id is None:
        return item.deleted
    parent_state = await get_item_state(item.p_id)
    while not parent_state['deleted'] and parent_state['p_id'] is not None:
        parent_state = await get_item_state(parent_state['p_id'])
    return parent_state['deleted']


async def resolve_acl(item):
    if item.acl is not None or item.p_id is None:
        return item.acl
    parent_state = await get_item_state(item.p_id)
    while parent_state['acl'] is None and parent_state['p_id'] is not None:
        parent_state = await get_item_state(parent_state['p_id'])
    return parent_state['acl']
