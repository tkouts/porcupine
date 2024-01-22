import asyncio
import copy
import orjson
from methodtools import lru_cache
from typing import ClassVar, Type, Optional

from porcupine.hinting import TYPING
from porcupine.config.default import DEFAULTS
from porcupine.core.context import system_override
from porcupine.core.services import get_service
from porcupine.core import utils
from porcupine.datatypes import DataType, String
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
        externals_info = {}
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
                            externals_info[attr.storage_key] = attr.storage_info
            except AttributeError:
                continue
        cls.__schema__ = schema
        cls.__record__ = storage(cls.__name__, field_spec)
        cls.__ext_record__ = storage(cls.__name__, ext_spec)
        cls.__sig__ = utils.hash_series(*schema.keys())
        cls.__externals_info__ = externals_info

        # add indexes
        if cls.is_collection:
            if hasattr(cls, 'indexes') and 'indexes' in cls.__dict__:
                DEFAULTS['__indices__'][cls] = cls.indexes
            if hasattr(cls, 'full_text_indexes') and \
                    'full_text_indexes' in cls.__dict__:
                DEFAULTS['__fts_indices__'][cls] = cls.full_text_indexes

        # register content class
        utils.ELASTIC_MAP[cls.__name__] = cls
        super().__init__(name, bases, dct)


class ElasticSlotsBase:
    __slots__ = (
        '__weakref__',
        '__storage__',
        '__externals__',
        '__snapshot__',
        '__is_new__',
        '_score'
    )


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
    __externals_info__: ClassVar[dict] = {}

    _id_size_: ClassVar[int] = 8
    is_collection: ClassVar[bool] = False
    is_composite: ClassVar[bool] = False
    is_deleted: ClassVar[bool] = False

    id = String(required=True, readonly=True)
    sig = String(required=True, readonly=True, protected=True)

    @staticmethod
    async def new_from_dict(dct: dict,
                            camel_to_snake=False) -> TYPING.ANY_ITEM_CO:
        item_type = dct.pop('type')
        if isinstance(item_type, str):
            # TODO: handle invalid type exception
            item_type = utils.get_content_class(item_type)
        new_item = item_type()
        await new_item.apply_patch(dct, camel_to_snake)
        return new_item

    @staticmethod
    def from_partial(partial):
        row = partial.raw_data
        content_class = partial.clazz
        storage = orjson.loads(row['data'])
        storage['id'] = row['id']
        storage['sig'] = row['sig']
        if not content_class.is_composite:
            storage['acl'] = partial.acl.to_json()
            storage['name'] = row['name']
            storage['cr'] = row['created']
            storage['md'] = row['modified']
            # params['is_collection'] = obj.is_collection
            storage['sys'] = row['is_system']
            storage['pid'] = row['parent_id']
            # params['p_type'] = dct.pop('_pcc', None)
            storage['exp'] = row['expires_at']
            storage['dl'] = row['is_deleted']
        return content_class(storage)

    @lru_cache(maxsize=None)
    @classmethod
    def view_attrs(cls):
        schema = cls.__schema__.values()
        return tuple([
            data_type.name for data_type in schema
            if not data_type.protected
            and data_type.storage == '__storage__'
        ])

    @lru_cache(maxsize=None)
    @classmethod
    def unique_data_types(cls):
        schema = cls.__schema__.values()
        return tuple([
            data_type for data_type in schema if data_type.unique
        ])

    def __init__(self, dict_storage: Optional[dict] = None, _score=0):
        self.__is_new__ = dict_storage is None or 'id' not in dict_storage
        self.__snapshot__ = {}
        self.__externals__ = self.__ext_record__()
        self._score = _score

        if self.__is_new__:
            # new item
            clazz = type(self)
            # initialize storage with default values
            self.__storage__ = self.__record__(clazz.__externals_info__)
            current_sig = clazz.__sig__
            self.__storage__.id = utils.generate_oid(self._id_size_)
            self.__storage__.sig = current_sig
        else:
            self.__storage__ = self.__record__(dict_storage)

    @property
    def friendly_name(self):
        return f'{self.id}({self.content_class})'

    def __reset__(self) -> None:
        self.__storage__.update(self.__snapshot__)
        self.__externals__.update(self.__snapshot__)
        self.__snapshot__ = {}

    def __repr__(self) -> str:
        _store = self.__storage__
        if self.__snapshot__:
            store = _store.as_dict()
            store.update({
                k: v for k, v in self.__snapshot__.items()
                if hasattr(_store, k)
            })
            return repr(self.__record__(store=store))
        else:
            return repr(_store)

    def get_snapshot_of(self, attr_name: str):
        if attr_name in self.__schema__:
            return self.__schema__[attr_name].get_value(self, snapshot=False)
        else:
            return getattr(self, attr_name)

    def to_json(self) -> dict:
        dct = {
            attr: getattr(self, attr)
            for attr in self.view_attrs()
        }
        dct['_type'] = self.content_class
        dct['_score'] = self._score
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
            args = self.view_attrs()
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

    @property
    async def ttl(self):
        raise NotImplementedError

    @property
    def has_outdated_schema(self):
        return self.__storage__.sig != type(self).__sig__

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
        if self.has_outdated_schema:
            await get_service('schema').clean_schema(self.id)

    async def on_post_delete(self, actor):
        ...
