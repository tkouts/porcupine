import asyncio
import copy

from porcupine import config
from porcupine.datatypes import DataType, String, ReferenceN
from porcupine.core.datatypes.system import SchemaSignature
from porcupine.utils import system
from porcupine.core.context import system_override
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
                    field_spec.append(attr.storage_key)
                    if isinstance(attr, ReferenceN):
                        field_spec.append(
                            system.get_active_chunk_key(attr.storage_key))
                        ext_spec.append(attr.storage_key)
                    if attr.indexed:
                        config.add_index(attr)
            except AttributeError:
                continue
        cls.__schema__ = schema
        cls.__record__ = storage(cls.__name__, field_spec)
        cls.__ext_record__ = storage(cls.__name__, ext_spec)
        cls.__sig__ = system.hash_series(*schema.keys())
        super().__init__(name, bases, dct)


class ElasticSlotsBase:
    __slots__ = ('__storage__', '_ext', '_snap', '__is_new__')


class Elastic(ElasticSlotsBase, metaclass=ElasticMeta):
    """
    Base class for all Porcupine objects.
    Accommodates schema updates without requiring database updates.
    The object schema is automatically updated the next time the
    object is written in the database.

    @cvar event_handlers: A list containing all the object's event handlers.
    @type event_handlers: list
    """
    __schema__ = {}
    __sig__ = ''
    __record__ = None
    __ext_record__ = None

    is_collection = False
    is_deleted = False
    event_handlers = []

    id = String(readonly=True)
    parent_id = String(readonly=True, allow_none=True,
                       default=None, store_as='pid')
    sig = SchemaSignature()

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
            self.__storage__.id = system.generate_oid()
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
    def __externals__(self):
        if self._ext is None:
            self._ext = self.__ext_record__()
        return self._ext

    def __reset__(self):
        self._snap = {}

    def __repr__(self):
        return repr(self.__storage__)

    def __add_defaults(self, data_types):
        for dt in data_types:
            dt.set_default(self)

    def get_snapshot_of(self, attr_name):
        return self.__snapshot__.get(
            attr_name,
            getattr(self.__storage__, attr_name))

    def to_json(self):
        schema = list(self.__schema__.values())
        return {attr.name: attr.__get__(self, None)
                for attr in schema
                if attr.protected is False
                and attr.storage == '__storage__'}

    # ujson hook
    toDict = to_json

    @property
    def content_class(self) -> str:
        """
        The fully qualified name of the object's class including the module.

        @rtype: str
        """
        return '{0}.{1}'.format(self.__class__.__module__,
                                self.__class__.__name__)

    def custom_view(self, *args, **kwargs) -> dict:
        result = {
            key: getattr(self, key) for key in args
        }
        if kwargs:
            result.update(kwargs)
        return result

    async def clone(self, memo=None):
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
        new_id = memo['_id_map_'].setdefault(self.id, system.generate_oid())
        clone = copy.deepcopy(self)
        # call data types clone method
        for dt in self.__schema__.values():
            _ = dt.clone(clone, memo)
            if asyncio.iscoroutine(_):
                await _
        with system_override():
            clone.id = new_id
            clone.parent_id = None
        clone.__is_new__ = True
        return clone
