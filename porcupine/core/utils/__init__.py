"""
Porcupine utilities package
"""

import inspect
import cbor
import functools
import hashlib
import random
from typing import Optional, AsyncIterator

from porcupine.hinting import TYPING
from porcupine import db
from porcupine.core.utils.collections import WriteOnceDict
from .permissions import resolve

VALID_ID_CHARS = [
    chr(x) for x in
    list(range(ord('a'), ord('z'))) +
    list(range(ord('A'), ord('Z'))) +
    list(range(ord('0'), ord('9')))]

ELASTIC_MAP = WriteOnceDict()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def generate_oid(length: int=8) -> str:
    """
    Generates a random Object ID string.

    @rtype: str
    """
    return ''.join(random.sample(VALID_ID_CHARS, length))


def get_content_class(name: str):
    return ELASTIC_MAP[name]


@functools.lru_cache(maxsize=None)
def get_rto_by_name(name: str):
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


def hash_series(*args, using='md5') -> str:
    b = cbor.dumps(args)
    hash_provider = getattr(hashlib, using)
    h = hash_provider(b)
    return h.hexdigest()


def get_attribute_lock_key(item_id: str, attr_name: str) -> str:
    return 'lck_{0}_{1}'.format(item_id, attr_name)


def get_blob_key(item_id: str, blob_name: str) -> str:
    return '{0}/{1}'.format(item_id, blob_name)


def get_active_chunk_key(collection_name: str) -> str:
    return '{0}_'.format(collection_name)


def get_collection_key(item_id: str, collection_name: str,
                       chunk_no: int) -> str:
    return '{0}/{1}/{2}'.format(item_id, collection_name, chunk_no)


def get_key_of_unique(parent_id: str, attr_name: str, attr_value) -> str:
    return '{0}>{1}>{2}'.format(parent_id, attr_name, hash_series(attr_value))


def get_composite_path(parent_path: str, comp_name: str) -> str:
    return '{0}.{1}'.format(parent_path, comp_name)


async def resolve_visibility(item: TYPING.ANY_ITEM_CO, user) -> Optional[int]:
    is_stale = await item.is_stale
    if is_stale:
        # TODO: remove from DB
        return None
    is_deleted = item.is_deleted
    if inspect.isawaitable(is_deleted):
        is_deleted = await is_deleted
    if is_deleted:
        return None
    # return user role
    return await resolve(item, user)


async def multi_with_stale_resolution(
        ids: TYPING.ID_LIST) -> AsyncIterator[TYPING.ANY_ITEM_CO]:
    async for i in db.get_multi(ids, return_none=True):
        if i is None:
            # TODO: remove stale id
            pass
        else:
            yield i


def get_descriptor_by_storage_key(cls, key: str):
    if key in cls.__schema__:
        return cls.__schema__[key]
    return locate_descriptor_by_storage_key(cls, key)


@functools.lru_cache(maxsize=None)
def locate_descriptor_by_storage_key(cls, key):
    for desc in cls.__schema__.values():
        if desc.storage_key == key:
            return desc
