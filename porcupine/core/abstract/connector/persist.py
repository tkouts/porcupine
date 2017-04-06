"""
Default database object persistence layer
"""
from porcupine.utils import system


class DefaultPersistence(object):
    @staticmethod
    def loads(value):
        content_class = system.get_rto_by_name(value.pop('c/c'))
        item = content_class(dict_storage=value)
        return item

    @staticmethod
    def dumps(obj):
        dct = obj.__storage__.as_dict()
        dct['c/c'] = obj.content_class
        return dct
