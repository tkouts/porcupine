import asyncio
import copy
from typing import List, ClassVar

from porcupine.hinting import TYPING
from porcupine import config
from porcupine.core.context import system_override
from porcupine.core.datatypes.system import SchemaSignature
from porcupine.core import utils
from porcupine.datatypes import DataType, String, ReferenceN
from porcupine.core.datatypes.asyncsetter import AsyncSetter
from .storage import storage


class ElasticMeta(type):
    def __new__(mcs, name, bases, dct):
        if '__slots__' not in dct:
            dct['__slots__'] = ()
        return super().__new__(mcs, name, bases, dct)

    def __init__(cls, name, bases, dct):
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
                    if attr.indexed:
                        config.add_index(attr)
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
    __slots__ = ('__storage__', '_ext', '_snap', '__is_new__')


class Elastic(ElasticSlotsBase, metaclass=ElasticMeta):
    """
    Base class for all Porcupine objects.
    Accommodates schema updates without requiring database updates.
    The object schema is automatically updated the next time the
    object is written in the database.

    :cvar event_handlers: A list containing all the object's event handlers.
    :type event_handlers: list
    :cvar is_collection: A boolean indicating if the object is a container.
    :type is_collection: bool
    """
    __schema__: TYPING.SCHEMA = {}
    __sig__: ClassVar[str] = ''
    __record__: TYPING.STORAGE = None
    __ext_record__: TYPING.STORAGE = None

    is_collection: ClassVar[bool] = False
    is_deleted: ClassVar[bool] = False
    event_handlers = []

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

    def __init__(self, dict_storage=None):
        if dict_storage is None:
            dict_storage = {}
        self._ext = None
        self._snap = None
        self.__is_new__ = 'id' not in dict_storage

        if self.__is_new__:
            # new item
            self.__storage__ = self.__record__(**dict_storage)
            # initialize storage with default values
            self.__add_defaults(list(self.__schema__.values()))
            self.__storage__.id = utils.generate_oid()
            self.__storage__.sig = type(self).__sig__
        elif dict_storage['sig'] == type(self).__sig__:
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
    def __snapshot__(self):
        if self._snap is None:
            self._snap = {}
        return self._snap

    @property
    def __externals__(self) -> storage:
        if self._ext is None:
            self._ext = self.__ext_record__()
        return self._ext

    def __reset__(self) -> None:
        self._snap = {}

    def __repr__(self) -> str:
        return repr(self.__storage__)

    def __add_defaults(self, data_types: List[DataType]) -> None:
        for dt in data_types:
            dt.set_default(self)

    def get_snapshot_of(self, attr_name: str):
        return self.__snapshot__.get(
            attr_name,
            getattr(self.__storage__, attr_name))

    def to_json(self) -> dict:
        schema = list(self.__schema__.values())
        return self.custom_view(*[data_type.name for data_type in schema
                                  if not data_type.protected
                                  and data_type.storage == '__storage__'])

    # ujson hook
    toDict = to_json

    @property
    def content_class(self) -> str:
        """
        The fully qualified name of the object's class including the module.

        @rtype: str
        """
        return type(self).__name__

    async def apply_patch(self, patch: dict) -> None:
        for attr, value in patch.items():
            if isinstance(self.__schema__.get(attr, None), AsyncSetter):
                await getattr(self, attr).reset(value)
            else:
                setattr(self, attr, value)

    def custom_view(self, *args, **kwargs) -> dict:
        result = {
            key: getattr(self, key) for key in args
        }
        if kwargs:
            result.update(kwargs)
        return result

    @property
    async def is_stale(self):
        raise NotImplementedError

    def reset(self):
        data_types = list(self.__schema__.values())
        for data_type in data_types:
            if not data_type.readonly \
                    and data_type.name != 'acl' \
                    and data_type.storage == '__storage__':
                setattr(self, data_type.name, data_type.default)

    async def clone(self, memo: dict=None) -> 'Elastic':
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
