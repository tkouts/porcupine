from functools import partial
from aiostream import stream, pipe

from porcupine import db
from porcupine.exceptions import OqlError
from porcupine.core.oql.tokens import Field, Variable, FunctionCall, \
    is_expression


class BaseStatement:
    funcs = {
        'len': lambda i, x: len(x),
        'slice': lambda i, x, start, end: x[start:end],
        'hasattr': hasattr,
    }

    @staticmethod
    def get_var(v, name):
        try:
            return v[name]
        except KeyError:
            raise OqlError(f'Unknown variable "{name}"')

    def call_func(self, func, *args):
        return self.funcs[func](*args)

    def prepare(self):
        raise NotImplementedError

    def execute(self, variables):
        raise NotImplementedError


class Select(BaseStatement):
    def __init__(self, scope, select_list):
        self.scope = scope
        self.select_list = select_list
        self.where_condition = None
        self.computed_fields = {}

    def get_field(self, i, name, var_map):
        if name in self.computed_fields:
            return self.computed_fields[name](i, self, var_map)
        return getattr(i, name)

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
            self.where_condition = self.where_condition.compile()

    def extract_fields(self, i, v):
        return {
            field_spec.alias: field_spec.expr(i, self, v)
            for field_spec in self.select_list
        }

    async def execute(self, variables):
        scope = self.scope.item_id
        if isinstance(scope, Variable):
            scope = self.get_var(variables, scope)

        item = await db.get_item(scope, quiet=False)
        # TODO: check we have a valid collection
        feeder = stream.iterate(getattr(item, self.scope.collection).items())

        if self.where_condition:
            flt = partial(self.where_condition, s=self, v=variables)
            feeder |= pipe.filter(flt)

        if self.select_list:
            field_extractor = partial(self.extract_fields, v=variables)
            feeder = feeder | pipe.map(field_extractor)

        async with feeder.stream() as streamer:
            return [result async for result in streamer]
