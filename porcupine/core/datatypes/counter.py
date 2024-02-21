from porcupine.core.context import context
from porcupine.connectors.mutations import SubDocument
from .common import Integer


class Counter(Integer):
    def __init__(self, default=0, **kwargs):
        super().__init__(default, required=False, **kwargs)

    async def on_change(self, instance, value, old_value):
        delta = value - old_value
        if delta and not instance.__is_new__:
            context.db.txn.mutate(
                instance,
                self.storage_key,
                SubDocument.COUNTER,
                delta
            )
