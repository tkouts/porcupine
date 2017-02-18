"""
Default database object persistence layer
"""
import marshal
from porcupine.utils import system


class DefaultPersistence(object):
    additional = ['is_collection']

    @staticmethod
    def loads(value):
        value = marshal.loads(marshal.dumps(value))
        content_class = system.get_rto_by_name(value['content/class'])
        item = content_class.__new__(content_class)
        item._dict = value
        return item

    @staticmethod
    def dumps(obj):
        dct = obj._dict
        for attr in DefaultPersistence.additional:
            if hasattr(obj, attr):
                dct[attr] = getattr(obj, attr)
        dct['content/class'] = obj.contentclass
        return dct
