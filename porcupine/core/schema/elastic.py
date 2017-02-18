from porcupine.datatypes import DataType, Composition
from porcupine.utils import system


class Elastic(object):
    """
    Base class for all Porcupine objects.
    Accommodates schema updates without requiring database updates.
    The object schema is automatically updated the next time the
    object is written in the database.

    @cvar event_handlers: A list containing all the object's event handlers.
    @type event_handlers: list
    """
    __schema__ = {}
    __sig__ = 0
    event_handlers = []

    def __new__(cls, *args, **kwargs):
        sig = hash(tuple(cls.__dict__.keys()))
        if cls.__sig__ != sig:
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
            cls.__sig__ = sig
        obj = super(Elastic, cls).__new__(cls)
        super(Elastic, obj).__setattr__('_dict', {
            'bag': {}
        })
        return obj

    def __init__(self):
        self._dict.update({
            '_id': system.generate_oid(),
            '_pid': None,
            '_is_deleted': 0,
            '_sig': hash(tuple(self.__schema__.keys()))
        })

    def __hash__(self):
        return hash(self._dict['_id'])

    def __getattr__(self, name):
        if name in self._dict:
            # attribute
            return self._dict[name]
        else:
            raise AttributeError(
                "'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name in self._dict:
            self._dict[name] = value
        else:
            super(Elastic, self).__setattr__(name, value)

    def __delattr__(self, name):
        try:
            del self._dict[name]
        except KeyError:
            super(Elastic, self).__delattr__(name)

    def __repr__(self):
        return str(self._dict)

    @property
    def id(self) -> str:
        """
        The ID of the object

        @rtype: str
        """
        return self._dict['_id']

    @property
    def parent_id(self) -> str:
        """
        The ID of the parent container

        @rtype: str
        """
        return self._dict['_pid']

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
        if '_sig' not in self._dict or self._dict['_sig'] != new_sig:
            item_schema = set(self._dict['bag'].keys())
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
                attr_value = self._dict['bag'].pop(attr_name)
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
            self._dict['_sig'] = new_sig
