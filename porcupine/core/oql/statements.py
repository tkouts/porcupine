from inspect import isawaitable
from functools import partial

from porcupine import db, log, pipe
from porcupine.core.stream.streamer import IdStreamer
from porcupine.core.oql.feeder import CollectionFeeder
from porcupine.core.oql.tokens import Field, FunctionCall, Variable, \
    is_expression


class BaseStatement:
    def prepare(self):
        raise NotImplementedError

    def execute(self, variables):
        raise NotImplementedError


class Select(BaseStatement):
    __slots__ = (
        'scope', 'select_list',
        'where_condition', 'order_by',
        'range', 'computed_fields',
        'where_compiled', 'order_by_compiled',
        'stale'
    )

    def __init__(self, scope, select_list):
        self.scope = scope
        self.select_list = select_list
        self.where_condition = None
        self.order_by = None
        self.range = None
        self.computed_fields = {}
        self.where_compiled = None
        self.order_by_compiled = None
        self.stale = 'update_after'

    def prepare(self):
        unnamed_expressions = 0
        for field_spec in self.select_list:
            # prepare aliases
            if field_spec.alias is None:
                if isinstance(field_spec.expr, Field):
                    field_spec.alias = str(field_spec.expr)
                else:
                    unnamed_expressions += 1
                    field_spec.alias = f'expr{unnamed_expressions}'

            is_computed = is_expression(field_spec.expr) or \
                isinstance(field_spec.expr, FunctionCall)
            field_spec.expr = field_spec.expr.compile()
            if is_computed:
                self.computed_fields[field_spec.alias] = field_spec.expr

        self.scope.target = self.scope.target.compile()

        if self.where_condition is not None:
            self.where_compiled = self.where_condition.compile()

        if self.order_by is not None:
            self.order_by_compiled = self.order_by.expr.compile()

        if self.range is not None:
            self.range.low = self.range.low.compile()
            self.range.high = self.range.high.compile()

    def extract_fields(self, i, v):
        return {
            field_spec.alias: field_spec.expr(i, self, v)
            for field_spec in self.select_list
        }

    async def execute(self, variables):
        scope, collection = self.scope(self, variables)
        # get scope
        item = await db.get_item(scope, quiet=False)
        scope_type = item.__class__

        stale = self.stale
        if isinstance(self.stale, Variable):
            stale = variables[stale]

        feeder = None

        if self.where_condition is not None:
            feeder = self.where_condition.optimize(scope_type, stale=stale)

        order_by = None
        results_ordered = False
        if self.order_by is not None:
            order_by = self.order_by.expr
            if feeder is None:
                if order_by == 'is_collection':
                    results_ordered = True
                else:
                    index_type = order_by.get_index_type(scope_type)
                    if index_type is not None:
                        feeder = order_by.get_index_lookup(index_type,
                                                           stale=stale)
                        results_ordered = True

        if feeder is None:
            # full scan
            feeder = CollectionFeeder(item, collection)

        select_range = None
        if self.range is not None:
            select_range = self.range(self, variables)

        apply_range_prematurely = False
        if order_by is not None and order_by == feeder.ordered_by:
            if self.order_by.desc and not feeder.reversed:
                feeder.reversed = True
            if self.range is not None:
                apply_range_prematurely = True

        log.debug(f'Feeder: {feeder}, {results_ordered}')

        has_optimized_feeder = not isinstance(feeder, CollectionFeeder)

        feeder = feeder(self, scope, variables)

        if isawaitable(feeder):
            feeder = await feeder

        if isinstance(feeder, IdStreamer):
            feeder = feeder.items()

        if has_optimized_feeder and collection in {'items', 'containers'}:
            if collection == 'items':
                feeder |= pipe.filter(lambda i: not i.is_collection)
            else:
                feeder |= pipe.filter(lambda i: i.is_collection)

        if self.where_condition is not None:
            flt = partial(self.where_compiled, s=self, v=variables)
            feeder |= pipe.filter(flt)

        # print(apply_range_prematurely)
        if apply_range_prematurely:
            feeder |= pipe.getitem(select_range)

        if self.order_by is not None and not results_ordered:
            key = partial(self.order_by_compiled, s=self, v=variables)
            feeder |= pipe.key_sort(key, _reverse=self.order_by.desc)

        if select_range is not None and not apply_range_prematurely:
            feeder |= pipe.getitem(select_range)

        if self.select_list:
            field_extractor = partial(self.extract_fields, v=variables)
            feeder = feeder | pipe.map(field_extractor)

        return [result async for result in feeder]
