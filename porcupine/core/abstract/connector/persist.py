"""
Default database object persistence layer
"""
from porcupine.utils import system


class DefaultPersistence(object):
    additional = ()

    @staticmethod
    def loads(value):
        content_class = system.get_rto_by_name(value.pop('c/c'))
        item = content_class(dict_storage=value)
        return item

    @staticmethod
    def dumps(obj):
        dct = obj.__storage__.as_dict()
        for attr in DefaultPersistence.additional:
            if hasattr(obj, attr):
                dct[attr] = getattr(obj, attr)
        dct['c/c'] = obj.content_class
        return dct
