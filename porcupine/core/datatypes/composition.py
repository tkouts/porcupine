"""
Porcupine composition data types
================================
"""
from porcupine import db, exceptions, context
from .reference import ReferenceN, ItemCollection
from .datatype import DataType
from porcupine.utils import system

Composite = None


class EmbeddedCollection(ItemCollection):
    async def add(self, item):
        if not item.__is_new__:
            raise TypeError('Can only add new items to composition')
        await super().add(item)
        context.txn.insert(item)

    def remove(self, item):
        super().remove(item)
        context.txn.delete(item)


class Composition(ReferenceN):
    """
    This data type is used for embedding a list composite objects to
    the assigned content type.

    @see: L{porcupine.schema.Composite}
    """
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return EmbeddedCollection(self, instance)

    async def clone(self, instance, memo):
        composites = await self.__get__(instance, None).items()
        self.__set__(instance, [item.clone(memo) for item in composites])

    async def on_create(self, instance, value):
        for composite in value:
            if not composite.__is_new__:
                # TODO: revisit
                raise TypeError('Can only add new items to composition')
            context.txn.insert(composite)
        await super().on_create(instance, [c.__storage__.id for c in value])

    async def on_change(self, instance, value, old_value):
        for composite in value:
            if composite.__is_new__:
                context.txn.insert(composite)
            else:
                context.txn.upsert(composite)
        await super().on_change(instance,
                                [c.__storage__.id for c in value],
                                None)

    async def on_delete(self, instance, value):
        composites = await self.__get__(instance, None).items()
        for composite in composites:
            context.txn.delete(composite)
        await super().on_delete(instance, value)


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
        elif isinstance(value, str):
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
                value._id = '{0}.{1}.{2}'.format(
                    instance.id, self.name, value.id)
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
