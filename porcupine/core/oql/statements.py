from functools import partial

from porcupine import db, log, pipe
from porcupine.core.stream.streamer import IdStreamer
from porcupine.core.oql.runtime import get_var
from porcupine.core.oql.tokens import Field, Variable, FunctionCall, \
    is_expression


class BaseStatement:
    def prepare(self):
        raise NotImplementedError

    def execute(self, variables):
        raise NotImplementedError


class Select(BaseStatement):
    def __init__(self, scope, select_list):
        self.scope = scope
        self.select_list = select_list
        self.where_condition = None
        self.order_by = None
        self.range = None
        self.computed_fields = {}
        self.optimized_feeder = None
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

        if self.where_condition:
            self.optimized_feeder = self.where_condition.optimize()
            self.where_condition = self.where_condition.compile()

        order_by = None
        if self.order_by:
            if self.order_by.expr.is_indexed:
                order_by = self.order_by.expr
                if self.optimized_feeder is None:
                    self.optimized_feeder = self.order_by.expr.get_index_lookup()
                    if self.order_by.desc:
                        self.optimized_feeder.reversed = True
            self.order_by.expr = self.order_by.expr.compile()

        if self.range and self.optimized_feeder \
                and order_by == self.optimized_feeder.sort_order:
            self.apply_range_prematurely = True

        log.debug(f'Optimized Feeder: {self.optimized_feeder}')

    def extract_fields(self, i, v):
        return {
            field_spec.alias: field_spec.expr(i, self, v)
            for field_spec in self.select_list
        }

    async def execute(self, variables):
        scope = self.scope.item_id
        if isinstance(scope, Variable):
            scope = get_var(variables, scope)

        item = await db.get_item(scope, quiet=False)
        # TODO: check we have a valid collection
        feeder = getattr(item, self.scope.collection)
        if self.optimized_feeder:
            feeder = self.optimized_feeder(self, scope, variables)

        if isinstance(feeder, IdStreamer):
            feeder = feeder.items()

        if self.where_condition:
            flt = partial(self.where_condition, s=self, v=variables)
            feeder |= pipe.filter(flt)
            # TODO: filter based on is_collection

        # print(self.apply_range_prematurely)
        if self.apply_range_prematurely:
            feeder |= pipe.getitem(self.range)

        if self.order_by:
            key = partial(self.order_by.expr, s=self, v=variables)
            feeder = (
                feeder |
                pipe.key_sort(key, reverse=self.order_by.desc) |
                pipe.flatten()
            )

        if self.range and not self.apply_range_prematurely:
            feeder |= pipe.getitem(self.range)

        if self.select_list:
            field_extractor = partial(self.extract_fields, v=variables)
            feeder = feeder | pipe.map(field_extractor)

        return [result async for result in feeder]
