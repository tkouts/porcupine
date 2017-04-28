import random
import hashlib
import functools
import collections
import cbor


VALID_ID_CHARS = [
    chr(x) for x in
    list(range(ord('a'), ord('z'))) +
    list(range(ord('A'), ord('Z'))) +
    list(range(ord('0'), ord('9')))
]


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


@functools.lru_cache()
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
    return '{0}/{1}/{2}'.format(parent_id, attr_name, hash_series(attr_value))


def resolve_set(raw_string: str) -> (list, float):
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
