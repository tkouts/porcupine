import asyncio
import copy
import functools
from typing import List, ClassVar, Type

from porcupine.hinting import TYPING
from porcupine.config.default import DEFAULTS
from porcupine.core.context import system_override
from porcupine.core import utils
from porcupine.datatypes import DataType, String, ReferenceN
from porcupine.core.datatypes.system import SchemaSignature
from porcupine.core.datatypes.asyncsetter import AsyncSetter
from .storage import storage


class ElasticMeta(type):
    def __new__(mcs, name, bases, dct):
        if '__slots__' not in dct:
            dct['__slots__'] = ()
        return super().__new__(mcs, name, bases, dct)

    def __init__(cls: Type['Elastic'], name, bases, dct):
        schema = {}
        field_spec = []
        ext_spec = []
        for attr_name in dir(cls):
            try:
                attr = getattr(cls, attr_name)
                if isinstance(attr, DataType):
                    schema[attr_name] = attr
                    attr.name = attr_name
                    if attr.storage == '__storage__':
                        field_spec.append(attr.storage_key)
                    else:
                        ext_spec.append(attr.storage_key)
                        if hasattr(attr, 'storage_info'):
                            field_spec.append(attr.storage_key)
                        if isinstance(attr, ReferenceN):
                            field_spec.append(
                                utils.get_active_chunk_key(attr.storage_key))
                    if cls.is_composite and (attr.unique or attr.indexed):
                        raise TypeError(f"Data type '{attr.name}' "
                                        f"of composite '{cls.__name__}' "
                                        "cannot be unique or indexed")
                    if attr.indexed:
                        DEFAULTS['__indices__'][attr.name] = attr
            except AttributeError:
                continue
        cls.__schema__ = schema
        cls.__record__ = storage(cls.__name__, field_spec)
        cls.__ext_record__ = storage(cls.__name__, ext_spec)
        cls.__sig__ = utils.hash_series(*schema.keys())
        # register content class
        utils.ELASTIC_MAP[cls.__name__] = cls
        super().__init__(name, bases, dct)


class ElasticSlotsBase:
    __slots__ = '__storage__', '__externals__', '__snapshot__', '__is_new__'


class Elastic(ElasticSlotsBase, metaclass=ElasticMeta):
    """
    Base class for all Porcupine objects.
    Accommodates schema updates without requiring database updates.
    The object schema is automatically updated the next time the
    object is written in the database.

    :cvar is_collection: A boolean indicating if the object is a container.
    :type is_collection: bool
    """
    __schema__: TYPING.SCHEMA = {}
    __sig__: ClassVar[str] = ''
    __record__: TYPING.STORAGE = None
    __ext_record__: TYPING.STORAGE = None

    _id_size_: ClassVar[int] = 8
    is_collection: ClassVar[bool] = False
    is_composite: ClassVar[bool] = False
    is_deleted: ClassVar[bool] = False

    id = String(required=True, readonly=True)
    sig = SchemaSignature()

    @staticmethod
    async def new_from_dict(dct: dict) -> TYPING.ANY_ITEM_CO:
        item_type = dct.pop('type')
        if isinstance(item_type, str):
            # TODO: handle invalid type exception
            item_type = utils.get_content_class(item_type)
        new_item = item_type()
        await new_item.apply_patch(dct)
        return new_item

    @classmethod
    @functools.lru_cache(maxsize=None)
    def view_data_types(cls):
        schema = cls.__schema__.values()
        return [data_type for data_type in schema
                if not data_type.protected
                and data_type.storage == '__storage__']

    @classmethod
    @functools.lru_cache(maxsize=None)
    def unique_data_types(cls):
        schema = cls.__schema__.values()
        return [data_type for data_type in schema if data_type.unique]

    def __init__(self, dict_storage=None):
        if dict_storage is None:
            dict_storage = {}
        self.__is_new__ = 'id' not in dict_storage
        self.__snapshot__ = {}
        self.__externals__ = self.__ext_record__()

        current_sig = type(self).__sig__

        if self.__is_new__:
            # new item
            self.__storage__ = self.__record__(**dict_storage)
            # initialize storage with default values
            self.__add_defaults(list(self.__schema__.values()))
            self.__storage__.id = utils.generate_oid(self._id_size_)
            self.__storage__.sig = current_sig
        elif dict_storage['sig'] == current_sig:
            self.__storage__ = self.__record__(**dict_storage)
        else:
            # construct new record that fits the new schema and the old one
            old_schema = tuple(dict_storage.keys())
            new_schema = self.__record__.fields()
            field_spec = frozenset(new_schema + old_schema)
            record = storage(type(self).__name__, field_spec)
            self.__storage__ = record(**dict_storage)
            # update storage with default values of added attributes
            additions = [dt for dt in self.__schema__.values()
                         if dt.storage_key not in old_schema]
            self.__add_defaults(additions)
            # change sig to trigger schema update
            with system_override():
                self.sig = str(id(self))

    @property
    def friendly_name(self):
        return '{0}({1})'.format(self.id, self.content_class)

    def __reset__(self) -> None:
        snapshot_items = self.__snapshot__.items()
        store = self.__storage__
        externals = self.__externals__
        store.update({
            k: v for k, v in snapshot_items
            if hasattr(store, k)
        })
        externals.update({
            k: v for k, v in snapshot_items
            if hasattr(externals, k)
        })
        self.__snapshot__ = {}

    def __repr__(self) -> str:
        _store = self.__storage__
        if self.__snapshot__:
            store = _store.as_dict()
            store.update({
                k: v for k, v in self.__snapshot__.items()
                if hasattr(_store, k)
            })
            return repr(self.__record__(**store))
        else:
            return repr(_store)

    def __add_defaults(self, data_types: List[DataType]) -> None:
        for dt in data_types:
            dt.set_default(self)

    def get_snapshot_of(self, attr_name: str):
        if attr_name in self.__schema__:
            storage_key = self.__schema__[attr_name].storage_key
            return getattr(self.__storage__, storage_key)
        else:
            return getattr(self, attr_name)

    def to_dict(self) -> dict:
        store = self.__storage__
        dct = {
            data_type.name: getattr(store,
                                    data_type.storage_key,
                                    data_type.default)
            for data_type in self.view_data_types()
        }
        dct['_type'] = self.content_class
        return dct

    @property
    def content_class(self) -> str:
        """
        The name of the object's class.

        @rtype: str
        """
        return type(self).__name__

    async def apply_patch(self, patch: dict, camel_to_snake=False) -> None:
        for attr, value in patch.items():
            if camel_to_snake:
                attr = utils.camel_to_snake(attr)
            if isinstance(self.__schema__.get(attr, None), AsyncSetter):
                await getattr(self, attr).reset(value)
            else:
                setattr(self, attr, value)

    def custom_view(self, *args, snake_to_camel=False, **kwargs) -> dict:
        if '*' in args:
            args = [dt.name for dt in self.view_data_types()]
        result = {
            utils.snake_to_camel(key)
            if snake_to_camel else key: getattr(self, key) for key in args
        }
        if kwargs:
            result.update(kwargs)
        return result

    @property
    async def effective_acl(self):
        raise NotImplementedError

    def reset(self):
        data_types = list(self.__schema__.values())
        for data_type in data_types:
            if not data_type.readonly \
                    and data_type.name != 'acl' \
                    and data_type.storage == '__storage__':
                setattr(self, data_type.name, data_type.default)

    async def clone(self, memo: dict = None) -> 'Elastic':
        """
        Creates an in-memory clone of the item.
        This is a shallow copy operation meaning that the item's
        references are not cloned.

        @return: the cloned object
        @rtype: L{Elastic}
        """
        if memo is None:
            memo = {
                '_dup_ext_': True,
                '_id_map_': {}
            }
        new_id = memo['_id_map_'].setdefault(self.id, utils.generate_oid())
        clone = copy.deepcopy(self)
        # call data types clone method
        for dt in self.__schema__.values():
            _ = dt.clone(clone, memo)
            if asyncio.iscoroutine(_):
                await _
        with system_override():
            clone.id = new_id
        clone.__is_new__ = True
        return clone

    # event handlers
    async def on_create(self):
        ...

    async def on_change(self):
        ...

    async def on_delete(self):
        ...

    async def on_post_create(self, actor):
        ...

    async def on_post_change(self, actor):
        ...

    async def on_post_delete(self, actor):
        ...
