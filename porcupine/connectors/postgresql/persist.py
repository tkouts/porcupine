"""
libsql database object persistence layer
"""
from copy import copy
import orjson
from collections import OrderedDict
from porcupine import log
from porcupine.core.schemaregistry import get_content_class
from porcupine.core import utils


def loads(row):
    # print('LOADING', row)
    try:
        content_class = get_content_class(row['type'])
    except KeyError:
        log.error(
            'Unable to determine content class of document \n'
            f'{row}'
        )
        raise
    storage = orjson.loads(row['data'])
    storage['id'] = row['id']
    storage['sig'] = row['sig']
    storage['expires_at'] = row['expires_at']
    if not content_class.is_composite:
        acl = row['acl']
        storage['acl'] = acl and orjson.loads(acl)
        storage['name'] = row['name']
        storage['created'] = row['created']
        storage['modified'] = row['modified']
        storage['is_system'] = bool(row['is_system'])
        storage['parent_id'] = row['parent_id']
        storage['p_type'] = row['p_type']
        storage['is_deleted'] = row['is_deleted']
    else:
        storage['parent_id'] = row['parent_id']
        storage['p_type'] = row['p_type']

    return content_class(storage)


def dumps(obj, read_uncommitted=False):
    # print(type(obj), obj.__snapshot__)
    json_encoder = utils.default_json_encoder
    storage = obj.__storage__
    if read_uncommitted:
        storage = copy(storage)
        storage.update(obj.__snapshot__)
    dct = storage.as_dict()
    params = OrderedDict()
    params['id'] = dct.pop('id')
    params['sig'] = dct.pop('sig')
    params['type'] = obj.content_class
    if not obj.is_composite:
        acl = dct.pop('acl', None)
        params['acl'] = (
            orjson.dumps(acl).decode('utf-8')
            if acl is not None else None
        )
        params['name'] = dct.pop('name')
        created = dct.pop('created')
        params['created'] = (
            created if isinstance(created, str) else created.isoformat()
        )
        modified = dct.pop('modified')
        params['modified'] = (
            modified if isinstance(modified, str) else modified.isoformat()
        )
        params['is_collection'] = obj.is_collection
        params['is_system'] = dct.pop('is_system', False)
        params['parent_id'] = dct.pop('parent_id', None)
        params['p_type'] = dct.pop('p_type', None)
        params['expires_at'] = dct.pop('expires_at', None)
        params['is_deleted'] = dct.pop('is_deleted', 0)
    else:
        params['parent_id'] = dct.pop('parent_id')
        params['p_type'] = dct.pop('p_type')
        params['expires_at'] = dct.pop('expires_at', None)
        params['is_deleted'] = dct.pop('is_deleted', 0)

    params['data'] = orjson.dumps(dct, default=json_encoder).decode('utf-8')
    # print(params)
    return params
