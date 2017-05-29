from porcupine import context, db
from .common import Integer


class Counter(Integer):
    def __init__(self, default=0, **kwargs):
        type_error_message = "{0}() got an unexpected keyword argument '{1}'"
        for kwarg in ('unique', 'indexed', 'required'):
            if kwarg in kwargs:
                raise TypeError(
                    type_error_message.format(self.__class__.__name__, kwarg))
        super().__init__(default, **kwargs)

    def on_change(self, instance, value, old_value):
        delta = value - old_value
        if delta and not instance.__is_new__:
            context.txn.mutate(instance,
                               self.storage_key,
                               db.connector.SUB_DOC_COUNTER,
                               delta)
