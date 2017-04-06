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
        module_name = modules[0]
        attribute = None
    else:
        module_name = '.'.join(modules[:-1])
        attribute = modules[-1]
    is_second_level = False

    try:
        mod = __import__(module_name, globals(), locals(),
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


def hash_series(*args, using='md5', salt=b''):
    with io.BytesIO() as bt:
        for arg in args:
            if arg is not None:
                if isinstance(arg, str):
                    arg = arg.encode('utf-8')
                bt.write(arg)
        hash_provider = getattr(hashlib, using)
        if salt:
            h = hash_provider(bt.getvalue(), salt=salt)
        else:
            h = hash_provider(bt.getvalue())
    return h


def resolve_set(raw_string):
    # build set
    uniques = {}
    dirty_count = 0
    total_count = 0
    # print('raw value is', raw_string)
    for oid in raw_string.split(' '):
        if oid:
            total_count += 1
            if oid.startswith('-'):
                key = oid[1:]
                if key in uniques:
                    dirty_count += 2
                    del uniques[key]
            else:
                if oid in uniques:
                    dirty_count += 1
                uniques[oid] = None
    value = list(uniques.keys())
    if total_count:
        dirtiness = dirty_count / total_count
    else:
        dirtiness = 0.0
    return value, dirtiness


def get_descriptor_by_storage_key(cls, key):
    if key in cls.__schema__:
        return cls.__schema__[key]
    return locate_descriptor_by_storage_key(cls, key)


@functools.lru_cache()
def locate_descriptor_by_storage_key(cls, key):
    for desc in cls.__schema__.values():
        if desc.storage_key == key:
            return desc


@context_cacheable(1000)
async def get_item_state(item_id):
    return await db.connector.get_partial(
        item_id, 'pid', 'acl', 'dl', 'sys',
        snapshot=True)


@context_cacheable(100)
async def resolve_deleted(item_id):
    state = await get_item_state(item_id)
    while not state['dl'] and state['pid'] is not None:
        if state['sys']:
            break
        state = await get_item_state(state['pid'])
    return state['dl']


@context_cacheable(100)
async def resolve_acl(object_id):
    state = await get_item_state(object_id)
    while state['acl'] is None and state['pid'] is not None:
        state = await get_item_state(state['pid'])
    return state['acl']
