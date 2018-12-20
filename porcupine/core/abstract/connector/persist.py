"""
Default database object persistence layer
"""
from porcupine.core import utils


class DefaultPersistence:
    @staticmethod
    def loads(storage):
        content_class = utils.get_content_class(storage.pop('_cc'))
        return content_class(dict_storage=storage)

    @staticmethod
    def dumps(obj):
        dct = dict(obj.__storage__.as_dict())
        dct['_cc'] = obj.content_class
        return dct
