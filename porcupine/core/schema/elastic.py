from porcupine import db, exceptions
from porcupine import config
from porcupine.contract import contract
from porcupine.datatypes import DataType, Composition, String, ReferenceN
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
                        field_spec.append('{0}_'.format(attr.storage_key))
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
            dict_storage['id'] = system.generate_oid()
            dict_storage['sig'] = self.__class__.__sig__
            self.__storage__ = self.__record__(**dict_storage)
            # initialize storage with default values
            self.__add_defaults()
        elif dict_storage['sig'] == self.__class__.__sig__:
            self.__storage__ = self.__record__(**dict_storage)
        else:
            # construct new record that fits the new schema and the old one
            field_spec = frozenset(self.__record__.fields() +
                                   tuple(dict_storage.keys()))
            record = storage(type(self).__name__, field_spec)
            self.__storage__ = record(**dict_storage)
            # update storage with default values of added attributes
            self.__add_defaults()
            # change sig to trigger schema update
            try:
                with system_override():
                    self.sig = str(id(self))
            except ValueError:
                # running outside context
                # possibly instantiated from schema maintenance service
                pass

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

    def __add_defaults(self):
        to_add = [
            dt for dt in list(self.__schema__.values())
            if getattr(getattr(self, dt.storage), dt.storage_key) is None
        ]
        for dt in to_add:
            dt.set_default(self)

    def to_dict(self):
        schema = list(self.__schema__.values())
        return {attr.name: attr.__get__(self, None)
                for attr in schema
                if attr.protected is False
                and attr.storage == '__storage__'}

    # json serializer
    toDict = to_dict

    @property
    def content_class(self) -> str:
        """
        The fully qualified name of the object's class including the module.

        @rtype: str
        """
        return '{0}.{1}'.format(self.__class__.__module__,
                                self.__class__.__name__)

    def custom_view(self, *args, **kwargs):
        if not args and not kwargs:
            return self

        result = {
            key: getattr(self, key) for key in args
        }
        if kwargs:
            result.update(kwargs)
        return result

    def get(self, request):
        return self

    @contract(accepts=dict)
    @db.transactional()
    async def patch(self, request):
        for attr, value in request.json.items():
            try:
                setattr(self, attr, value)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        await self.update()
