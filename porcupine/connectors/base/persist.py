"""
Default database object persistence layer
"""
import json
from porcupine import context, log
from porcupine.core.schemaregistry import get_content_class


def loads(storage):
    try:
        content_class = get_content_class(storage['_cc'])
    except KeyError:
        log.error(
            'Unable to determine content class of document \n'
            f'{json.dumps(storage, indent=4)}'
        )
        raise
    item_meta_cache = context.item_meta
    item_meta = {}
    if not content_class.is_composite:
        item_meta['_score'] = item_meta_cache.get(storage['id'], 0)
    return content_class(storage, **item_meta)


def dumps(obj):
    dct = obj.__storage__.as_dict()
    dct['_cc'] = obj.content_class
    if not obj.is_composite:
        dct['_col'] = obj.is_collection
    return dct
