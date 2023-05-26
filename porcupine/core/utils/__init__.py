"""
Porcupine utilities package
"""
import cbor
import hashlib
import random
import re
from typing import Union

import mmh3
from methodtools import lru_cache

from porcupine.core.context import context
from porcupine.core.utils.collections import WriteOnceDict
from porcupine.core.utils.date import DATE_TYPES

VALID_ID_CHARS = [
    chr(x) for x in
    list(range(ord('a'), ord('z'))) +
    list(range(ord('A'), ord('Z'))) +
    list(range(ord('0'), ord('9')))
]

ELASTIC_MAP = WriteOnceDict()


def default_json_encoder(obj):
    if obj.__class__ in DATE_TYPES:
        return obj.isoformat()
    elif hasattr(obj, 'to_json'):
        return obj.to_json()


def generate_oid(length: int = 8) -> str:
    """
    Generates a random Object ID string.

    @rtype: str
    """
    return ''.join(random.choice(VALID_ID_CHARS) for _ in range(length))


def get_content_class(name: str):
    return ELASTIC_MAP[name]


@lru_cache(maxsize=None)
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


def hash_series(*args, using='md5') -> Union[str, int]:
    if len(args) == 1 and isinstance(args[0], str):
        b = args[0].encode()
    else:
        b = cbor.dumps(args)
    if using == 'mmh3':
        return mmh3.hash(b) & 0xffffffff
    else:
        hash_provider = getattr(hashlib, using)
        h = hash_provider(b)
        return h.hexdigest()


def get_attribute_lock_key(item_id: str, attr_name: str) -> str:
    return f'lck_{item_id}_{attr_name}'  # .format(item_id, attr_name)


def get_blob_key(item_id: str, blob_name: str) -> str:
    return f'{item_id}/{blob_name}'  # .format(item_id, blob_name)


def get_active_chunk_key(collection_name: str) -> str:
    return f'{collection_name}_'  # .format(collection_name)


def get_collection_key(item_id: str, collection_name: str,
                       chunk_no: int) -> str:
    return f'{item_id}/{collection_name}/{chunk_no}'


def get_key_of_unique(parent_id: str, attr_name: str, attr_value) -> str:
    return f'{parent_id}>{attr_name}>{hash_series(attr_value, using="mmh3")}'


def get_composite_path(parent_path: str, comp_name: str) -> str:
    return f'{parent_path}.{comp_name}'  # .format(parent_path, comp_name)


def get_descriptor_by_storage_key(cls, key: str):
    if key in cls.__schema__:
        return cls.__schema__[key]
    return locate_descriptor_by_storage_key(cls, key)


@lru_cache(maxsize=None)
def locate_descriptor_by_storage_key(cls, key):
    for desc in cls.__schema__.values():
        if desc.storage_key == key:
            return desc


async def add_uniques(item):
    parent_id = item.parent_id
    if parent_id is not None:
        txn = context.txn
        # insert unique keys
        item_id = item.id
        ttl = await item.ttl
        for unique in item.unique_data_types():
            unique_key = get_key_of_unique(parent_id, unique.name,
                                           unique.get_value(item))
            txn.insert_external(unique_key, item_id, ttl)


def remove_uniques(item):
    parent_id = item.get_snapshot_of('parent_id')
    is_deleted = item.get_snapshot_of('is_deleted')
    if parent_id is not None and not is_deleted:
        txn = context.txn
        # remove unique keys
        for unique in item.unique_data_types():
            unique_key = get_key_of_unique(
                parent_id,
                unique.name,
                item.get_snapshot_of(unique.name))
            txn.delete_external(unique_key)


_re1 = re.compile('(.)([A-Z][a-z]+)')
_re2 = re.compile('([a-z0-9])([A-Z])')


def camel_to_snake(s):
    s1 = re.sub(_re1, r'\1_\2', s)
    return re.sub(_re2, r'\1_\2', s1).lower()


def snake_to_camel(s, init_cap=False):
    camel = ''.join(char.capitalize() for char in s.split('_'))
    if not init_cap:
        camel = camel[:1].lower() + camel[1:]
    return camel
