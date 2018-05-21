from porcupine import context, db
from .common import Integer


class Counter(Integer):
    def __init__(self, default=0, **kwargs):
        super().__init__(default, unique=False, required=False, **kwargs)

    def on_change(self, instance, value, old_value):
        delta = value - old_value
        if delta and not instance.__is_new__:
            context.txn.mutate(instance, self.storage_key,
                               db.connector.SUB_DOC_COUNTER, delta)
