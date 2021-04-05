"""
Default database object persistence layer
"""
from porcupine import context
from porcupine.core import utils


def loads(storage):
    content_class = utils.get_content_class(storage.pop('_cc'))
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
