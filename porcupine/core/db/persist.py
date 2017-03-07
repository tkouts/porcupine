"""
Default database object persistence layer
"""
import marshal
from porcupine.utils import system


class DefaultPersistence(object):
    additional = []

    @staticmethod
    def loads(value):
        value = marshal.loads(marshal.dumps(value))
        content_class = system.get_rto_by_name(value.pop('c/c'))
        item = content_class(storage=value)
        return item

    @staticmethod
    def dumps(obj):
        dct = obj.__storage__
        for attr in DefaultPersistence.additional:
            if hasattr(obj, attr):
                dct[attr] = getattr(obj, attr)
        dct['c/c'] = obj.content_class
        return dct
