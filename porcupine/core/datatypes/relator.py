"""
Porcupine reference data types
==============================
"""
from functools import cached_property

from pypika import Parameter, Table, Field, Query
from methodtools import lru_cache

from porcupine import exceptions, db
from porcupine.contract import contract
from porcupine.core.context import context, system_override
from porcupine.connectors.schematables import ItemsTable
from porcupine.connectors.postgresql.query import QueryType, PorcupineQuery
from .reference import Reference, Acceptable, ItemReference
from .collection import ItemCollection
from .asyncsetter import AsyncSetter
from .mutable import List


class RelatorBase:
    def __init__(self, rel_attr, respects_references):
        if not rel_attr:
            raise exceptions.SchemaError(
                'Relator must specify its related attribute'
            )
        self.rel_attr = rel_attr
        self.respects_references = respects_references


class RelatorItem(ItemReference):
    def __new__(cls, value, descriptor):
        s = super().__new__(cls, value)
        s._desc = descriptor
        return s

    async def item(self, quiet=True):
        item = await super().item(quiet=quiet)
        if not item or self._desc.rel_attr not in item.__schema__:
            return None
        return item


class Relator1(Reference, RelatorBase):
    """
    This data type is used whenever an item possibly references another item.
    The referenced item B{IS} aware of the items that reference it.

    @cvar rel_attr: contains the name of the attribute of the referenced
                   content classes. The type of the referenced attribute should
                   be B{strictly} be a L{Relator1} or L{RelatorN}
                   data type for one-to-one and one-to-many relationships
                   respectively.
    @type rel_attr: str

    @var respects_references: if set to C{True} then the object cannot be
                              deleted if there are objects that reference it.
    @type respects_references: bool

    @var cascade_delete: if set to C{True} then all the object referenced
                         will be deleted upon the object's deletion.
    @type cascade_delete: bool
    """
    def __init__(self, default=None, rel_attr=None, respects_references=False,
                 **kwargs):
        super().__init__(default, **kwargs)
        RelatorBase.__init__(self, rel_attr, respects_references)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value:
            return RelatorItem(value, self)

    # async def on_create(self, instance, value):
    #     ref_item = await super().on_create(instance, value)
    #     if ref_item:
    #         await self.add_reference(instance, ref_item)

    # async def on_change(self, instance, value, old_value):
    #     ref_item = await super().on_change(instance, value, old_value)
    #     if ref_item:
    #         await self.add_reference(instance, ref_item)
    #     if old_value:
    #         old_ref_item = await db_connector().get(old_value)
    #         if old_ref_item:
    #             await self.remove_reference(instance, old_ref_item)

    async def on_delete(self, instance, value):
        await super().on_delete(instance, value)
        if value and not self.cascade_delete:
            ref_item = await context.db.get(value)
            if ref_item:
                if self.respects_references:
                    raise exceptions.Forbidden(
                        f'{instance.friendly_name} can not be '
                        'removed because is referenced by other items.')
                # await self.remove_reference(instance, ref_item)


class RelatorN(AsyncSetter, List, Acceptable, RelatorBase):
    """
    This data type is used whenever an item references none, one or more items.
    The referenced items B{ARE} aware of the items that reference them.

    @cvar rel_attr: the name of the attribute of the referenced
                    content classes.
                    The type of the referenced attribute should be B{strictly}
                    be a subclass of L{Relator1} or L{RelatorN} data types for
                    one-to-many and many-to-many relationships respectively.
    @type rel_attr: str

    @cvar respects_references: if set to C{True} then the object
                               cannot be deleted if there are objects that
                               reference it.
    @type respects_references: bool

    @cvar cascade_delete: if set to C{True} then all the objects referenced
                         will be deleted upon the object's deletion.
    @type cascade_delete: bool
    """
    safe_type = list, tuple
    storage = '__externals__'
    # storage_info_prefix = '_relN_'

    def __init__(
        self,
        default=(),
        accepts=(),
        rel_attr=None,
        cascade_delete=False,
        respects_references=False,
        **kwargs
    ):
        Acceptable.__init__(self, accepts, cascade_delete)
        RelatorBase.__init__(self, rel_attr, respects_references)
        super(List, self).__init__(
            default,
            allow_none=False,
            store_as=None,
            **kwargs
        )
        self.t = ItemsTable(self)

    async def clone(self, instance, memo):
        collection = self.__get__(instance, None).items()
        # TODO: run as system to fetch all collection items
        super(List, self).__set__(
            instance,
            [item async for item in collection]
        )

    def getter(self, instance, value=None):
        return ItemCollection(self, instance)

    @cached_property
    def is_many_to_many(self):
        allowed_types = self.allowed_types
        if allowed_types:
            return isinstance(getattr(allowed_types[0],
                                      self.rel_attr), RelatorN)
        return False

    @cached_property
    def associative_table(self):
        if self.is_many_to_many:
            ref_class = self.allowed_types[0]
            other_relator = getattr(ref_class, self.rel_attr)
            classes = [
                f'{ref_class.__name__.lower()}_{self.rel_attr}',
                f'{other_relator.allowed_types[0].__name__.lower()}_{self.name}'
            ]
            classes.sort()
            return Table('_x_'.join(classes))

    @property
    def associative_table_fields(self):
        if self.is_many_to_many:
            fields = [
                self.join_field.name,
                self.equality_field.name
            ]
            fields.sort()
            return fields

    @property
    def join_field(self):
        if self.is_many_to_many:
            ref_class = self.allowed_types[0]
            return getattr(
                self.associative_table,
                f'{ref_class.__name__.lower()}_id'
            )

    @property
    def equality_field(self):
        if self.is_many_to_many:
            ref_class = self.allowed_types[0]
            this_class = getattr(ref_class, self.rel_attr).allowed_types[0]
            return getattr(
                self.associative_table,
                f'{this_class.__name__.lower()}_id'
            )

    @lru_cache(maxsize=None)
    def query(self, query_type=QueryType.ITEMS):
        # Query = db_connector().Query
        if self.is_many_to_many:
            if query_type is QueryType.RAW_ASSOCIATIVE:
                q = (
                    Query
                    .from_(self.associative_table,
                           wrap_set_operation_queries=False)
                    .select()
                    .where(
                        self.equality_field == Parameter(':instance_id')
                    )
                )
            else:
                q = (
                    Query
                    .from_(self.associative_table,
                           wrap_set_operation_queries=False)
                    .join(self.t)
                    .on(self.join_field == self.t.id)
                    .select()
                    .where(
                        self.equality_field == Parameter(':instance_id')
                    )
                )
        else:
            rel_attr = self.rel_attr
            q = (
                Query
                .from_(self.t,
                       wrap_set_operation_queries=False)
                .select()
                .where(
                    getattr(self.t, rel_attr) == Parameter(':instance_id')
                )
            )
        if query_type is QueryType.ITEMS:
            columns = self.t.columns + ('data', )
            q = q.select(*[Field(name, table=self.t) for name in columns])
        elif query_type is QueryType.PARTIAL:
            q = q.select(*self.t.partial_fields)
        # print(q)
        return PorcupineQuery(q, query_type=query_type)

    @staticmethod
    async def can_add(instance, *items):
        return await instance.can_update(context.user)

    can_remove = can_add

    async def on_delete(self, instance, value):
        if self.cascade_delete:
            collection = self.__get__(instance, None)
            with system_override():
                async for ref_item in collection.items():
                    await ref_item.remove()

        await super().on_delete(instance, value)

    async def on_recycle(self, instance, value):
        await super().on_recycle(instance, value)
        collection = self.__get__(instance, None)
        if self.cascade_delete:
            with system_override():
                async for ref_item in collection.items():
                    # mark as deleted
                    ref_item.is_deleted += 1
                    await context.db.txn.upsert(ref_item)
                    await context.db.txn.recycle(ref_item)

    async def on_restore(self, instance, value):
        await super().on_restore(instance, value)
        collection = self.__get__(instance, None)
        if self.cascade_delete:
            with system_override():
                async for ref_item in collection.items():
                    await ref_item.restore()

    # HTTP views

    def get_member_id(self, instance, request_path):
        chunks = request_path.split(f'{instance.id}/{self.name}')
        member_id = chunks[-1]
        if member_id.startswith('/'):
            member_id = member_id[1:]
        return member_id

    async def get(self, instance, request, expand=False):
        expand = expand or 'expand' in request.args
        member_id = self.get_member_id(instance, request.path)
        collection = getattr(instance, self.name)
        if member_id:
            member = await collection.get_member_by_id(member_id, quiet=False)
            return member
        else:
            if expand:
                return await collection.items().list()
            return await collection.ids()

    @contract(accepts=str)
    @db.transactional()
    async def post(self, instance, request):
        """
        Adds an item to the collection
        :param instance:
        :param request:
        :return:
        """
        if self.get_member_id(instance, request.path):
            raise exceptions.MethodNotAllowed('Method not allowed')
        item = await db.get_item(request.json, quiet=False)
        collection = getattr(instance, self.name)
        try:
            await collection.add(item)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        return True

    @contract(accepts=list)
    @db.transactional()
    async def put(self, instance, request):
        """
        Adds an item to the collection
        :param instance:
        :param request:
        :return:
        """
        collection = await super().put(instance, request)
        return await collection.ids()

    @db.transactional()
    async def delete(self, instance, request):
        member_id = self.get_member_id(instance, request.path)
        collection = getattr(instance, self.name)
        if not member_id:
            raise exceptions.MethodNotAllowed('Method not allowed')
        member = await collection.get_member_by_id(member_id, quiet=False)
        await collection.remove(member)

    # async def on_create(self, instance, value):
    #     added, _ = await super().on_create(instance, value)
    #     if added:
    #         await self.add_reference(instance, *added)

    # async def on_change(self, instance, value, old_value):
    #     added, removed = await super().on_change(instance, value, old_value)
    #     if added:
    #         await self.add_reference(instance, *added)
    #     if removed:
    #         await self.remove_reference(instance, *removed)

    # async def on_delete(self, instance, value):
    #     collection = self.__get__(instance, None)
    #     if not self.cascade_delete:
    #         with system_override():
    #             async for ref_item in collection.items():
    #                 if self.respects_references:
    #                     raise exceptions.Forbidden(
    #                         f'{instance.friendly_name} can not be '
    #                         'removed because is referenced by other items.')
    #                 # await self.remove_reference(instance, ref_item)
    #     # remove collection documents
    #     await super().on_delete(instance, value)
