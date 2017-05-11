"""
Default database object persistence layer
"""
from porcupine.utils import system


class DefaultPersistence(object):
    @staticmethod
    def loads(value):
        content_class = system.get_content_class(value.pop('_cc'))
        item = content_class(dict_storage=value)
        return item

    @staticmethod
    def dumps(obj):
        dct = obj.__storage__.as_dict()
        dct['_cc'] = obj.content_class
        return dct
