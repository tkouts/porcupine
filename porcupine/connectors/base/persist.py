"""
Default database object persistence layer
"""
from porcupine import context
from porcupine.core import utils


class DefaultPersistence:
    @staticmethod
    def loads(storage):
        content_class = utils.get_content_class(storage.pop('_cc'))
        return content_class(
            storage,
            _score=context.item_meta.get(storage['id'])
        )

    @staticmethod
    def dumps(obj):
        dct = obj.__storage__.as_dict()
        dct['_cc'] = obj.content_class
        if not obj.is_composite:
            dct['_col'] = obj.is_collection
        return dct
