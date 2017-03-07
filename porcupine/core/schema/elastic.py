from porcupine.datatypes import DataType, Composition, String, Integer
from porcupine.utils import system
from porcupine.core.context import system_override


class Elastic:
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
    __is_new__ = False
    event_handlers = []

    id = String(readonly=True)
    p_id = String(readonly=True)
    is_deleted = Integer(readonly=True)
    sig = String(readonly=True)

    def __new__(cls, storage=None):
        if not cls.__sig__:
            schema = {}
            for attr_name in dir(cls):
                try:
                    attr = getattr(cls, attr_name)
                    if isinstance(attr, DataType):
                        schema[attr_name] = attr
                        attr.name = attr_name
                except AttributeError:
                    continue
            cls.__schema__ = schema
            cls.__sig__ = system.hash_series(*schema.keys()).hexdigest()
        obj = super(Elastic, cls).__new__(cls)
        return obj

    def __init__(self, storage=None):
        if storage is None:
            storage = {}
        self.__storage__ = storage
        self.__externals__ = {}
        self.__snapshot__ = {}
        if 'id' not in storage:
            # new item
            self.__is_new__ = True
            # initialize storage with default values

            storage['id'] = system.generate_oid()
            storage['sig'] = self.__class__.__sig__
        else:
            # update sig to latest schema
            with system_override():
                self.sig = self.__class__.__sig__

    def __hash__(self):
        return hash(self.__storage__['id'])

    def __repr__(self):
        return str(self.__storage__)

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
        return '{}.{}'.format(self.__class__.__module__,
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
