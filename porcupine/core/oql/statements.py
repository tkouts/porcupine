import logging
from functools import partial

from aiostream import stream

from porcupine import db, log, pipe
from porcupine.core.oql.feeder import CollectionFeeder
from porcupine.core.oql.tokens import (
    Field,
    FunctionCall,
    Variable,
    is_expression
)


class BaseStatement:
    def prepare(self):
        raise NotImplementedError

    def execute(self, variables):
        raise NotImplementedError


class Select(BaseStatement):
    __slots__ = (
        'select_list',
        'computed_fields',
        'scope',
        'where_condition',
        'where_compiled',
        'order_by',
        'order_by_compiled',
        'range',
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
        debug_log = log.level <= logging.DEBUG
        scope, collection = self.scope(self, variables)
        # get scope
        item = await db.get_item(scope, quiet=False)

        stale = self.stale
        if isinstance(self.stale, Variable):
            stale = variables[stale]

        # default feeder / scan all
        feeder = CollectionFeeder()

        order_by_fields = None
        if self.order_by and self.order_by.fields:
            order_by_fields = self.order_by.fields
        # print(order_by_fields)

        if self.where_condition is not None:
            feeder = self.where_condition.optimize(item.__class__,
                                                   order_by_fields,
                                                   stale=stale) or feeder

        select_range = None
        if self.range is not None:
            select_range = self.range(self, variables)

        results_ordered = False
        if self.order_by is not None:
            if feeder.is_ordered_by(order_by_fields):
                results_ordered = True
            elif not feeder.optimized:
                expr = self.order_by.expr
                index_lookup = expr.optimize(item.__class__,
                                             order_by_fields,
                                             stale=stale)
                if index_lookup is not None:
                    feeder = index_lookup
                    results_ordered = True
            if self.order_by.desc:
                feeder.reversed = True
        else:
            results_ordered = True

        if debug_log:
            log.debug(f'Feeder: {feeder}, {results_ordered}')

        streamer = feeder(self, item, collection, variables)

        if debug_log:
            log.debug(f'Streamer: {streamer}')

        if streamer.output_ids:
            streamer = streamer.items()

        if feeder.optimized and collection in {'items', 'containers'}:
            if collection == 'items':
                streamer |= pipe.filter(lambda i: not i.is_collection)
            else:
                streamer |= pipe.filter(lambda i: i.is_collection)

        if self.where_condition is not None:
            flt = partial(self.where_compiled, s=self, v=variables)
            streamer |= pipe.filter(flt)

        if results_ordered:
            if select_range is not None:
                # print('premature range')
                streamer |= pipe.getitem(select_range)
        else:
            if self.order_by is not None:
                key = partial(self.order_by_compiled, s=self, v=variables)
                streamer |= pipe.key_sort(key, _reverse=self.order_by.desc)

            if select_range is not None:
                # print('mature range')
                streamer |= pipe.getitem(select_range)

        if self.select_list:
            field_extractor = partial(self.extract_fields, v=variables)
            streamer |= pipe.map(field_extractor)

        return await stream.list(streamer)
