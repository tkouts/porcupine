"""
libsql database object persistence layer
"""
# import json
import orjson
from collections import OrderedDict
from porcupine import context, log
from porcupine.core import utils


def loads(row):
    # print(row)
    try:
        content_class = utils.get_content_class(row['type'])
    except KeyError:
        log.error(
            'Unable to determine content class of document \n'
            f'{row}'
        )
        raise
    storage = orjson.loads(row['data'])
    storage['id'] = row['id']
    storage['sig'] = row['sig']
    if not content_class.is_composite:
        # acl = row['acl']
        storage['name'] = row['name']
        storage['cr'] = row['created']
        storage['md'] = row['modified']
        # params['is_collection'] = obj.is_collection
        # storage['acl'] = acl and orjson.loads(acl)
        storage['pid'] = row['parent_id']
        # params['p_type'] = dct.pop('_pcc', None)
        storage['exp'] = row['expires_at']
        storage['dl'] = row['deleted']
    # item_meta_cache = context.item_meta
    # item_meta = {}
    # if not content_class.is_composite:
    #     item_meta['_score'] = item_meta_cache.get(storage['id'], 0)
    return content_class(storage)


def dumps(obj):
    json_encoder = utils.default_json_encoder
    dct = obj.__storage__.as_dict()
    params = OrderedDict()
    params['id'] = dct.pop('id')
    params['sig'] = dct.pop('sig')
    params['type'] = obj.content_class
    if not obj.is_composite:
        # acl = dct.pop('acl', None)
        params['name'] = dct.pop('name')
        params['created'] = dct.pop('cr').isoformat()
        params['modified'] = dct.pop('md').isoformat()
        params['is_collection'] = obj.is_collection
        # params['acl'] = orjson.dumps(acl).decode() if acl is not None else None
        params['parent_id'] = dct.pop('pid', None)
        params['p_type'] = dct.pop('_pcc', None)
        params['expires_at'] = dct.pop('exp', None)
        params['deleted'] = dct.pop('dl', 0)

    params['data'] = orjson.dumps(dct, default=json_encoder).decode()
    print(params)
    return params
