from porcupine import db, exceptions
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
            except AttributeError:
                continue
        cls.__schema__ = schema
        cls.__record__ = storage(cls.__name__, field_spec)
        cls.__ext_record__ = storage(cls.__name__, ext_spec)
        cls.__sig__ = system.hash_series(*schema.keys()).hexdigest()
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
    p_id = String(readonly=True, allow_none=True, default=None)
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
            # update storage with default values of added attributes
            self.__add_defaults()
            # update sig to latest schema
            with system_override():
                self.sig = self.__class__.__sig__

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
        to_add = [dt for dt in list(self.__schema__.values())
                  if getattr(getattr(self, dt.storage), dt.storage_key) is None]
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
    def parent_id(self):
        """
        The ID of the parent container

        @rtype: str
        """
        return self.p_id

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

    def update_schema(self):
        schema = self.__schema__
        new_sig = hash(tuple(schema.keys()))
        if 'sig' not in self.__storage__ or self.__storage__['sig'] != new_sig:
            item_schema = set(self.__storage__.keys())
            current_schema = set(schema.keys())

            for_addition = current_schema - item_schema
            for attr_name in for_addition:
                # add new attributes
                value = getattr(self, attr_name)
                # call event handlers
                getattr(self.__class__, attr_name).on_create(self, value)

            # remove old attributes
            for_removal = item_schema - current_schema
            composite_pid = ':{}'.format(self.id)
            for attr_name in for_removal:
                # detect if it is composite attribute
                attr_value = self.__storage__.pop(attr_name)
                if attr_value:
                    if isinstance(attr_value, str):
                        # is it an embedded data type?
                        item = db._db.get_item(attr_value)
                        if item is not None and item.parent_id == composite_pid:
                            Composition._remove_composite(item, True)
                    elif isinstance(attr_value, list):
                        # is it a composition data type?
                        item = db._db.get_item(attr_value[0])
                        if item is not None and item.parent_id == composite_pid:
                            items = db._db.get_multi(attr_value)
                            if all([item.parent_id == composite_pid
                                    for item in items]):
                                for item in items:
                                    Composition._remove_composite(item, True)
            # [self._dict['bag'].pop(x) for x in for_removal]
            self.__storage__['sig'] = new_sig
