import random
import inspect
import hashlib
import functools
import collections
import cbor
from typing import Optional
from porcupine import db
from .permissions import resolve


VALID_ID_CHARS = [
    chr(x) for x in
    list(range(ord('a'), ord('z'))) +
    list(range(ord('A'), ord('Z'))) +
    list(range(ord('0'), ord('9')))]


class FrozenDict(collections.Mapping):
    __slots__ = ('_dct', )

    def __init__(self, dct):
        self._dct = dct

    def __getitem__(self, item):
        return self._dct[item]

    def __iter__(self):
        return iter(self._dct)

    def __len__(self):
        return len(self._dct)

    def to_dict(self):
        return {**self._dct}

    toDict = to_dict


class WriteOnceDict(collections.MutableMapping, dict):
    # dict implementations to override the MutableMapping versions
    __getitem__ = dict.__getitem__
    __iter__ = dict.__iter__

    def __delitem__(self, key):
        raise KeyError('Read-only dictionary')

    def __setitem__(self, key, value):
        if key in self:
            raise KeyError('{0} has already been set'.format(key))
        dict.__setitem__(self, key, value)


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


async def fetch_collection_chunks(collection_key: str) -> (list, int):
    prev_chunks = []
    item_id, collection_name, chunk_no = collection_key.split('/')
    previous_chunk_no = int(chunk_no) - 1
    # fetch previous chunks
    while True:
        previous_chunk_key = get_collection_key(item_id,
                                                collection_name,
                                                previous_chunk_no)
        previous_chunk = await db.connector.get_raw(previous_chunk_key)
        if previous_chunk is not None:
            # print(len(previous_chunk))
            prev_chunks.insert(0, previous_chunk)
            previous_chunk_no -= 1
        else:
            break
    return prev_chunks, previous_chunk_no + 1


async def resolve_visibility(item, user) -> Optional[int]:
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


def resolve_set(raw_string: str) -> (list, float):
    # build set
    uniques = {}
    dirtiness = 0.0
    dirty_count = 0
    total_count = 0
    # print('raw value is', raw_string)
    for oid in raw_string.split(' '):
        if oid:
            total_count += 1
            if oid.startswith('-'):
                key = oid[1:]
                dirty_count += 1
                if key in uniques:
                    dirty_count += 1
                    del uniques[key]
            else:
                if oid in uniques:
                    dirty_count += 1
                uniques[oid] = None
    value = list(uniques.keys())
    if total_count:
        dirtiness = dirty_count / total_count
    return value, dirtiness


def get_descriptor_by_storage_key(cls, key):
    if key in cls.__schema__:
        return cls.__schema__[key]
    return locate_descriptor_by_storage_key(cls, key)


@functools.lru_cache(maxsize=None)
def locate_descriptor_by_storage_key(cls, key):
    for desc in cls.__schema__.values():
        if desc.storage_key == key:
            return desc
