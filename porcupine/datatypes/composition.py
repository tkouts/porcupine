"""
Porcupine composition data types
================================
"""
from porcupine import db, exceptions
from .common import DataType, List
from porcupine.utils import system

Composite = None


class Composition(List):
    """
    This data type is used for embedding a list composite objects to
    the assigned content type.

    @var composite_class: the name of the content class that can be embedded.

    @see: L{porcupine.schema.Composite}
    """
    composite_class = ''
    nested = False

    def __init__(self, default=None, **kwargs):
        if default is None:
            default = []
        super(Composition, self).__init__(default, **kwargs)
        if 'composite_class' in kwargs:
            self.composite_class = kwargs['composite_class']
        if 'nested' in kwargs:
            self.nested = kwargs['nested']

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super(Composition, self).__get__(instance, owner)
        if value:
            if isinstance(value[0], str):
                value = instance._dict['bag'][self.name] = \
                    db._db.get_multi(value)
            elif isinstance(value[0], dict):
                value = instance._dict['bag'][self.name] = [
                    db._db._persist.loads(item_dict) for item_dict in value]
            # super(Composition, self).__set__(instance, value)
        return value

    def clone(self, instance, memo):
        composites = self.__get__(instance, instance.__class__)
        if composites:
            self.__set__(instance, [item.clone(memo) for item in composites])

    def on_create(self, instance, value):
        self.on_update(instance, value, None)

    def on_update(self, instance, value, old_value):
        global Composite
        if Composite is None:
            from porcupine.schema import Composite
        nested = self.nested
        # variable to detect change from external -> nested
        should_remove_externals = False
        composites = {}

        if value:
            # load objects
            composite_type = system.get_rto_by_name(self.composite_class)

            for i, obj in enumerate(value):
                if isinstance(obj, Composite):
                    if nested and '.' not in obj.id:
                        obj._id = '%s.%s.%s' % (instance.id, self.name, obj.id)
                    elif not nested and '.' in obj.id:
                        obj._id = obj.id.split('.')[-1]
                    obj._pid = ':%s' % instance.id
                else:
                    raise exceptions.ContainmentError(
                        'Invalid object type "{}" in composition.'.format(
                            obj.__class__.__name__))
                composites[obj.id] = obj

            # check containment
            if [obj for obj in composites.values()
                    if not isinstance(obj, composite_type)]:
                raise exceptions.ContainmentError(
                    'Invalid content class "{}" in composition.'.format(
                        obj.contentclass))

        # get previous value
        if old_value is not None:
            old_ids = set([composite.id for composite in old_value])
            if old_ids:
                # detect schema change
                should_remove_externals = nested and '.' not in old_value[0].id
        else:
            old_ids = set()

        new_ids = set([obj.id for obj in value])

        # calculate removed composites
        removed = list(old_ids - new_ids)
        [Composition._remove_composite(
            [c for c in old_value if c.id == id][0], nested=nested)
         for id in removed]

        # calculate added composites
        added = list(new_ids - old_ids)
        for obj_id in added:
            db._db.handle_update(composites[obj_id], None)
            if not nested:
                db._db.put_item(composites[obj_id])

        # calculate constant composites
        constants = list(new_ids & old_ids)
        for obj_id in constants:
            db._db.handle_update(composites[obj_id],
                                 [c for c in old_value if c.id == obj_id][0])
            if not nested:
                db._db.put_item(composites[obj_id])

        if not nested:
            setattr(instance, self.name, [obj.id for obj in value])
        else:
            setattr(instance, self.name,
                    [db._db._persist.dumps(obj) for obj in value])
            if should_remove_externals:
                for c in old_value:
                    db._db.delete_item(c)

    def on_delete(self, instance, value, is_permanent):
        # do not rely on definition - inspect current schema
        # the schema will be updated when the item is restored
        nested = False
        if value:
            nested = '.' in value[0].id
        [Composition._remove_composite(composite, is_permanent, nested)
         for composite in value]
        # restore ids
        if not is_permanent:
            if not nested:
                setattr(instance, self.name, [obj.id for obj in value])
            else:
                setattr(instance, self.name,
                        [db._db._persist.dumps(obj) for obj in value])

    @staticmethod
    def _remove_composite(composite, permanent=True, nested=False):
        db._db.handle_delete(composite, permanent)
        composite._is_deleted = 1
        if not nested and '.' not in composite.id:
            if not permanent:
                db._db.put_item(composite)
            else:
                db._db.delete_item(composite)

    def on_undelete(self, instance, value):
        nested = self.nested
        for composite in value:
            composite._is_deleted = 0
            if nested and '.' not in composite.id:
                db._db.delete_item(composite)
                composite._id = '{}.{}.{}'.format(
                    instance.id, self.name, composite.id)
            elif not nested and '.' in composite.id:
                db._db.handle_delete(composite, True, execute_event_handlers=False)
                composite._id = composite.id.split('.')[-1]
                db._db.handle_update(composite, None, execute_event_handlers=False)

            db._db.handle_undelete(composite)
            if not nested:
                db._db.put_item(composite)

        # restore ids
        if not nested:
            setattr(instance, self.name, [obj.id for obj in value])
        else:
            setattr(instance, self.name,
                    [db._db._persist.dumps(obj) for obj in value])


class Embedded(DataType):
    """
    This data type is used for embedding a single composite objects to
    the assigned content type.

    @var composite_class: the name of the content class that can be embedded.

    @see: L{porcupine.schema.Composite}
    """
    safe_type = object
    allow_none = True
    composite_class = ''
    nested = False

    def __init__(self, default=None, **kwargs):
        super(Embedded, self).__init__(default, **kwargs)
        if 'composite_class' in kwargs:
            self.composite_class = kwargs['composite_class']
        if 'nested' in kwargs:
            self.nested = kwargs['nested']

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = DataType.__get__(self, instance, owner)
        if value is None:
            return None
        elif isinstance(value, basestring):
            value = instance._dict['bag'][self.name] = db._db.get_item(value)
        elif isinstance(value, dict):
            # nested
            value = instance._dict['bag'][self.name] = \
                db._db._persist.loads(value)
        return value

    def clone(self, instance, memo):
        embedded = self.__get__(instance, instance.__class__)
        if embedded is not None:
            self.__set__(instance, embedded.clone(memo))

    def on_create(self, instance, value):
        self.on_update(instance, value, None)

    def on_update(self, instance, value, old_value):
        global Composite
        if Composite is None:
            from porcupine.schema import Composite

        nested = self.nested
        # variable to detect change from external -> nested
        should_remove_external = False

        if value is not None:
            if isinstance(value, Composite):
                if nested and '.' not in value.id:
                    value._id = '%s.%s.%s' % (instance.id, self.name, value.id)
                elif not nested and '.' in value.id:
                    value._id = value.id.split('.')[-1]
                value._pid = ':%s' % instance.id
            else:
                raise exceptions.ContainmentError(
                    'Invalid object type "{}" in composition.'.format(
                        value.__class__.__name__))

            # check containment
            composite_type = system.get_rto_by_name(self.composite_class)

            if not isinstance(value, composite_type):
                raise exceptions.ContainmentError(
                    'Invalid content class "{}" in composition.'.format(
                        value.contentclass))

        # get previous value
        if old_value is not None:
            if (value and value.id) != old_value.id:
                Composition._remove_composite(old_value, nested=nested)
            # detect schema change
            should_remove_external = nested and '.' not in old_value.id

        if value is not None:
            is_new = value.id != (old_value and old_value.id)
            db._db.handle_update(value, None if is_new else old_value)
            if not nested:
                db._db.put_item(value)
                setattr(instance, self.name, value.id)
            else:
                setattr(instance, self.name, db._db._persist.dumps(value))

        if should_remove_external:
            db._db.delete_item(old_value)

    def on_delete(self, instance, value, is_permanent):
        if value is not None:
            # do not rely on definition - inspect current schema
            # the schema will be updated when the item is restored
            nested = '.' in value.id
            Composition._remove_composite(value, is_permanent, nested=nested)
            # restore correct format in bag
            if not is_permanent:
                if not nested:
                    setattr(instance, self.name, value.id)
                else:
                    setattr(instance, self.name, db._db._persist.dumps(value))

    def on_undelete(self, instance, value):
        if value is not None:
            nested = self.nested
            value._is_deleted = 0

            if nested and '.' not in value.id:
                db._db.delete_item(value)
                value._id = '{}.{}.{}'.format(instance.id, self.name, value.id)
            elif not nested and '.' in value.id:
                db._db.handle_delete(value, True, execute_event_handlers=False)
                value._id = value.id.split('.')[-1]
                db._db.handle_update(value, None, execute_event_handlers=False)

            db._db.handle_undelete(value)
            if not nested:
                db._db.put_item(value)

            # restore correct format in bag
            if not nested:
                setattr(instance, self.name, value.id)
            else:
                setattr(instance, self.name, db._db._persist.dumps(value))
