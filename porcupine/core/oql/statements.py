from inspect import isawaitable
from functools import partial

from porcupine import db, log, pipe
from porcupine.core.stream.streamer import IdStreamer
from porcupine.core.oql.feeder import CollectionFeeder
from porcupine.core.oql.tokens import Field, FunctionCall, is_expression


class BaseStatement:
    def prepare(self):
        raise NotImplementedError

    def execute(self, variables):
        raise NotImplementedError


class Select(BaseStatement):
    __slots__ = (
        'scope', 'select_list', 'where_condition', 'order_by',
        'range', 'computed_fields', 'feeder', 'apply_range_prematurely'
    )

    def __init__(self, scope, select_list):
        self.scope = scope
        self.select_list = select_list
        self.where_condition = None
        self.order_by = None
        self.range = None
        self.computed_fields = {}
        self.feeder = None
        self.apply_range_prematurely = False

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

        if self.where_condition:
            self.feeder = self.where_condition.optimize()
            self.where_condition = self.where_condition.compile()

        if self.feeder is None:
            self.feeder = CollectionFeeder(self.scope.collection)

        order_by = None
        if self.order_by:
            order_by = self.order_by.expr
            if order_by.is_indexed and isinstance(self.feeder,
                                                  CollectionFeeder):
                self.feeder = order_by.get_index_lookup()
            self.order_by.expr = order_by.compile()

        if self.range:
            self.range.low = self.range.low.compile()
            self.range.high = self.range.high.compile()

        if order_by == self.feeder.ordered_by:
            if self.order_by.desc != self.feeder.desc:
                self.feeder.reversed = True
            if self.range:
                self.apply_range_prematurely = True

        log.debug(f'Feeder: {self.feeder}')

    def extract_fields(self, i, v):
        return {
            field_spec.alias: field_spec.expr(i, self, v)
            for field_spec in self.select_list
        }

    async def execute(self, variables):
        scope, _ = self.scope(self, variables)

        select_range = None
        if self.range:
            select_range = self.range(self, variables)

        # print(select_range)

        if not isinstance(self.feeder, CollectionFeeder):
            # make sure scope is accessible
            await db.get_item(scope, quiet=False)

        feeder = self.feeder(self, scope, variables)
        if isawaitable(feeder):
            feeder = await feeder

        if isinstance(feeder, IdStreamer):
            feeder = feeder.items()

        if self.where_condition:
            flt = partial(self.where_condition, s=self, v=variables)
            feeder |= pipe.filter(flt)
            # TODO: filter based on is_collection

        # print(self.apply_range_prematurely)
        if self.apply_range_prematurely:
            feeder |= pipe.getitem(select_range)

        if self.order_by:
            key = partial(self.order_by.expr, s=self, v=variables)
            feeder = (
                feeder |
                pipe.key_sort(key, _reverse=self.order_by.desc) |
                pipe.flatten()
            )

        if select_range and not self.apply_range_prematurely:
            feeder |= pipe.getitem(select_range)

        if self.select_list:
            field_extractor = partial(self.extract_fields, v=variables)
            feeder = feeder | pipe.map(field_extractor)

        return [result async for result in feeder]
