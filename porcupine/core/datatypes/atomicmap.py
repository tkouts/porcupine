from porcupine import exceptions
from porcupine.core.services import db_connector
from porcupine.core.context import context
from porcupine.core.utils import collections
from porcupine.core.datatypes.mutable import Dictionary
from porcupine.core.datatypes.asyncsetter import AsyncSetter, AsyncSetterValue

IMMUTABLE_TYPES = (str, int, float, bool, tuple)


class AtomicMapValue(AsyncSetterValue, collections.FrozenDict):

    def __init__(self, descriptor: 'AtomicMap', instance, dct: dict):
        collections.FrozenDict.__init__(self, dct)
        AsyncSetterValue.__init__(self, descriptor, instance)

    async def set(self, key: str, value):
        descriptor, instance = self._desc, self._inst()
        if not await descriptor.can_modify(instance):
            raise exceptions.Forbidden('Forbidden')
        descriptor.validate_map_value(value)
        await instance.touch()
        self._dct[key] = value
        if not instance.__is_new__:
            context.txn.mutate(instance,
                               '{0}.{1}'.format(descriptor.storage_key, key),
                               db_connector().SUB_DOC_UPSERT_MUT,
                               value)

    async def delete(self, key: str):
        descriptor, instance = self._desc, self._inst()
        if not await descriptor.can_modify(instance):
            raise exceptions.Forbidden('Forbidden')
        await instance.touch()
        del self._dct[key]
        if not instance.__is_new__:
            context.txn.mutate(instance,
                               '{0}.{1}'.format(descriptor.storage_key, key),
                               db_connector().SUB_DOC_REMOVE, None)

    async def reset(self, value, replace=False):
        if replace and value is not None:
            value['__replace__'] = True
        await super().reset(value)

    def __getitem__(self, item):
        if self._dct is None:
            raise KeyError(item)
        return super().__getitem__(item)

    def __iter__(self):
        if self._dct is None:
            raise StopIteration
        return super().__iter__()

    def __len__(self):
        if self._dct is None:
            return 0
        return super().__len__()

    def to_json(self):
        if self._dct is None:
            return None
        return super().to_json()


class AtomicMap(AsyncSetter, Dictionary):

    def __init__(self, default=None, accepts=IMMUTABLE_TYPES, **kwargs):
        super().__init__(default, **kwargs)
        if accepts != IMMUTABLE_TYPES:
            for value_type in accepts:
                if value_type not in IMMUTABLE_TYPES:
                    raise TypeError(
                        'Atomic map value types should be immutable')
        self.accepts = accepts

    def getter(self, instance, value=None):
        if value is not None:
            return AtomicMapValue(self, instance, value)

    def validate_map_value(self, value):
        if not isinstance(value, self.accepts):
            raise TypeError(
                'Unsupported value type {0} for {1}'.format(
                    type(value).__name__,
                    self.name or type(self).__name__))
        if isinstance(value, tuple):
            # make sure tuple elements are immutable
            try:
                hash(value)
            except TypeError:
                raise TypeError(
                    'Tuple elements of atomic map {0} should be immutable'
                    .format(self.name or type(self).__name__))

    def validate_value(self, instance, value):
        super().validate_value(instance, value)
        if value is not None:
            for map_value in value.values():
                self.validate_map_value(map_value)

    async def on_change(self, instance, value, old_value):
        self.validate(value)
        if not instance.__is_new__:
            connector = db_connector()
            replace = value and value.pop('__replace__', False)
            if value is None or old_value is None or replace:
                context.txn.mutate(instance, self.storage_key,
                                   connector.SUB_DOC_UPSERT_MUT, value)
            else:
                changed_keys = [
                    key for key in value
                    if key not in old_value or
                    (key in old_value and value[key] != old_value[key])
                ]
                removed_keys = [
                    key for key in old_value
                    if key not in value
                ]
                for key in changed_keys:
                    path = '{0}.{1}'.format(self.storage_key, key)
                    context.txn.mutate(instance,
                                       path,
                                       connector.SUB_DOC_UPSERT_MUT,
                                       value[key])
                for key in removed_keys:
                    path = '{0}.{1}'.format(self.storage_key, key)
                    context.txn.mutate(instance,
                                       path,
                                       connector.SUB_DOC_REMOVE, None)
