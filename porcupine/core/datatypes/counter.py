import inspect
from porcupine import context, db
from .common import Integer


class Counter(Integer):
    ad_hoc = False

    def __init__(self, default=0, **kwargs):
        type_error_message = "{0}() got an unexpected keyword argument '{1}'"
        for kwarg in ('unique', 'indexed', 'required'):
            if kwarg in kwargs:
                raise TypeError(
                    type_error_message.format(self.__class__.__name__, kwarg))
        super().__init__(default, **kwargs)

    async def on_change(self, instance, value, old_value):
        if not instance.__is_new__:
            result = context.txn.mutate(instance,
                                        self.storage_key,
                                        db.connector.SUB_DOC_COUNTER,
                                        value - old_value,
                                        ad_hoc=self.ad_hoc)
            if inspect.isawaitable(result):
                result = await result
            return result[self.storage_key]
        if self.ad_hoc:
            return value
