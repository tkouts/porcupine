"""
Porcupine composition data types
================================
"""
from porcupine import db, exceptions, context
from porcupine.contract import contract
from porcupine.utils import system
from porcupine.core.context import system_override
from porcupine.core.schema.composite import Composite
from .reference import ReferenceN, ItemCollection
from .datatype import DataType


class EmbeddedCollection(ItemCollection):
    async def get_item_by_id(self, item_id):
        with system_override():
            return await super().get_item_by_id(item_id)

    async def items(self):
        with system_override():
            return await super().items()

    async def add(self, *composites):
        with system_override():
            await super().add(*composites)
            for composite in composites:
                if not composite.__is_new__:
                    raise TypeError('Can only add new items to composition')
                composite.parent_id = self._inst.id
                context.txn.insert(composite)
        await self._inst.update()

    async def remove(self, *composites):
        with system_override():
            await super().remove(*composites)
            for composite in composites:
                context.txn.delete(composite)
        await self._inst.update()


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
            with system_override():
                composite.parent_id = instance.id
            context.txn.insert(composite)
        with system_override():
            await super().on_create(instance, [c.__storage__.id for c in value])

    async def on_change(self, instance, value, old_value):
        old_ids = frozenset(await self.fetch(instance, set_storage=False))
        collection = self.__get__(instance, None)
        new_ids = frozenset([c.__storage__.id for c in value])
        removed_ids = old_ids.difference(new_ids)
        added = []

        with system_override():
            for composite in value:
                if composite.__is_new__:
                    composite.parent_id = instance.id
                    context.txn.insert(composite)
                    added.append(composite)
                else:
                    context.txn.upsert(composite)
            removed = await db.get_multi(removed_ids)
            for item in removed:
                context.txn.delete(item)
            await super(EmbeddedCollection, collection).remove(*removed)
            await super(EmbeddedCollection, collection).add(*added)

    async def on_delete(self, instance, value):
        composite_ids = await self.fetch(instance, set_storage=False)
        async for composite in db.connector.get_multi(composite_ids):
            context.txn.delete(composite)
        # remove collection documents
        await super().on_delete(instance, value)

    # HTTP views
    async def get(self, instance, request, expand=False):
        return await super().get(instance, request, expand=True)

    @contract(accepts=dict)
    @db.transactional()
    async def post(self, instance, request):
        """
        Adds a new composite to the collection
        :param instance: 
        :param request: 
        :return: 
        """
        collection = getattr(instance, self.name)
        item_dict = request.json
        item_dict.setdefault('type', self.allowed_types[0])
        try:
            composite = Composite.new_from_dict(item_dict)
            await collection.add(composite)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return composite

    @db.transactional()
    async def put(self, instance, request):
        composites = []
        collection = self.__get__(instance, None)
        for item_dict in request.json:
            composite_id = item_dict.pop('id', None)
            try:
                if composite_id:
                    item_dict.pop('type', None)
                    composite = await collection.get_item_by_id(composite_id)
                    composite.apply_patch(item_dict)
                else:
                    item_dict.setdefault('type', self.allowed_types[0])
                    composite = Composite.new_from_dict(item_dict)
                composites.append(composite)
            except exceptions.AttributeSetError as e:
                raise exceptions.InvalidUsage(str(e))
        setattr(instance, self.name, composites)
        await instance.update()
        return composites


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
