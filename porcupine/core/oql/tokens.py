from typing import Optional, Callable
from namedlist import namedlist
from functools import lru_cache

from porcupine import log
from porcupine.exceptions import OqlError
from porcupine.core.oql.runtime import environment
from porcupine.core.oql.feeder import *

__all__ = (
    'T_False',
    'T_True',
    'T_Null',
    'Integer',
    'Float',
    'String',
    'Field',
    'Variable',
    'FunctionCall',
    'FreeText',
    'Expression',
    'UnaryExpression',
    'DynamicSlice',
    'FieldSpec',
    'Scope',
    'OrderBy',
    'is_expression'
)


class Token:
    primitive = True
    immutable = True

    def __init__(self, value, *_args, **_kwargs):
        self.value = value
        self.__lambda = None

    def __repr__(self):
        return f'{self.__class__.__name__}({self.value})'

    def compile(self) -> Callable:
        if self.__lambda is None:
            code = f'lambda i, s, v: {self.source()}'
            self.__lambda = eval(code, environment)
        return self.__lambda

    def source(self):
        return repr(self.value)

    def optimize(self, item, collection, **options):
        return CollectionFeeder(item, collection)

    def __call__(self, statement, v):
        return self.value


T_True = Token(True)
T_False = Token(False)
T_Null = Token(None)


class Integer(Token, int):
    ...


class Float(Token, float):
    ...


class String(Token, str):
    ...


class Field(String):
    primitive = False
    immutable = False

    def source(self):
        if '.' in self:
            # nested attribute
            path = self.split('.')
            return f'get_nested_field(i, "{path[0]}", {path[1:]})'
        return f'get_field(i, "{self}", s, v)'

    @lru_cache(maxsize=None)
    def _get_indexed_container_type(self, container_type):
        for cls in container_type.mro():
            if 'indexes' in cls.__dict__:
                for attr_set in cls.indexes:
                    if attr_set == self:
                        return cls, attr_set
                    elif isinstance(attr_set, list) and attr_set[0] == self:
                        return cls, ','.join(attr_set)
        return None, None

    def optimize(self, item, collection, **options):
        indexed_type, index_name = self._get_indexed_container_type(
            item.__class__)
        if indexed_type is not None:
            index_lookup = IndexLookup(indexed_type, index_name,
                                       options=options)
            return index_lookup
        return CollectionFeeder(item, collection)


class Variable(Token, str):
    primitive = False

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'

    def source(self):
        return f'get_var(v, "{self}")'

    def __call__(self, statement, v):
        return environment['get_var'](v, self)


class FunctionCall(namedlist('FunctionCall', 'func, args'), Token):
    primitive = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Token.__init__(self, None)

    @property
    def immutable(self):
        return all((arg.immutable for arg in self.args))

    def source(self):
        # add item arg first
        args = ['i']
        if self.args:
            args.extend([arg.source() for arg in self.args])
        return f'{self.func}({", ".join(args)})'

    def __call__(self, statement, v):
        args = [arg(statement, v) for arg in self.args]
        return environment[self.func](None, *args)


class FreeText(namedlist('FreeText', 'field term'), Token):
    primitive = False
    immutable = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Token.__init__(self, True)

    def __hash__(self):
        return hash(self.field)

    @lru_cache(maxsize=None)
    def _get_indexed_container_type(self, container_type) -> Optional[type]:
        for cls in container_type.mro():
            if 'full_text_indexes' in cls.__dict__:
                if self.field == '*' or self in cls.full_text_indexes:
                    return cls
        return None

    def optimize(self, item, collection, **options):
        if not self.term.immutable:
            raise OqlError('FREETEXT term should be immutable')
        indexed_type = self._get_indexed_container_type(item.__class__)
        if indexed_type is not None:
            index_lookup = FTSIndexLookup(
                indexed_type,
                self.field,
                self.term,
                options=options
            )
            return index_lookup
        log.warn(f'Non indexed FTS lookup on {item.__class__.__name__}')
        return EmptyFeeder({})


OptimizedExpr = namedlist('OptimizedExpr', 'expr feeder')


class Expression(namedlist('Expression', 'l_op operator r_op'), Token):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Token.__init__(self, None)

    @property
    def primitive(self):
        return self.l_op.primitive and self.r_op.primitive

    @property
    def immutable(self):
        return self.l_op.immutable and self.r_op.immutable

    def source(self):
        source = f'{self.l_op.source()} {self.operator} {self.r_op.source()}'
        if self.primitive:
            return repr(eval(source))
        return source

    def __call__(self, statement, v):
        return self.compile()(None, statement, v)

    def optimize(self, item, collection, **options):
        # comparison
        is_comparison = self.operator in {'==', '>=', '<=', '>', '<'}
        if is_comparison:
            operator = self.operator
            bounds = None
            feeder = None
            if self.r_op.immutable:
                feeder = self.l_op.optimize(item, collection, **options)
                bounds = self.r_op
            elif self.l_op.immutable:
                # reverse operator
                if operator.startswith('>'):
                    operator = f'<{operator[1:]}'
                elif operator.startswith('<'):
                    operator = f'>{operator[1:]}'
                feeder = self.r_op.optimize(item, collection, **options)
                bounds = self.l_op

            if feeder and feeder.optimized:
                bounds = bounds.compile()
                if operator is not None:
                    if operator == '==':
                        # equality
                        feeder.bounds = bounds
                    elif operator.startswith('>'):
                        dynamic_range = DynamicRange(bounds)
                        dynamic_range.l_inclusive = operator == '>='
                        feeder.bounds = dynamic_range
                    elif operator.startswith('<'):
                        dynamic_range = DynamicRange(None, False, bounds)
                        dynamic_range.u_inclusive = operator == '<='
                        feeder.bounds = dynamic_range
                return feeder

        # logical
        is_logical = self.operator in {'and', 'or'}
        if is_logical:
            optimized = [
                OptimizedExpr(op, op.optimize(item, collection, **options))
                for op in (self.l_op, self.r_op)
            ]
            optimized.sort(key=lambda f: -f.feeder.priority)
            first, second = optimized
            if self.operator == 'and':
                # print(optimized)
                if first.feeder.optimized:
                    if second.feeder.optimized:
                        # intersection
                        return Intersection(*[o.feeder for o in optimized])
                    else:
                        first.feeder.filter_func = second.expr.compile()
                        return first.feeder
            if self.operator == 'or':
                # print(optimized)
                if first.feeder.optimized:
                    # union
                    if not second.feeder.optimized:
                        second.feeder.filter_func = second.expr.compile()
                        # print(second.expr.source())
                    return Union(*[o.feeder for o in optimized])

        # full scan
        return CollectionFeeder(item, collection)


class UnaryExpression(namedlist('UnaryExpression', 'operator operand'), Token):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Token.__init__(self, None)

    @property
    def primitive(self):
        return self.operand.primitive

    @property
    def immutable(self):
        return self.operand.immutable

    def source(self):
        spacer = ' ' if self.operator == 'not' else ''
        source = f'{self.operator}{spacer}{self.operand.source()}'
        if self.primitive:
            return repr(eval(source))
        return source


class Scope(namedlist('Scope', 'target collection')):
    def __call__(self, statement, v):
        return self.target(None, statement, v), self.collection


class DynamicSlice(namedlist('DynamicSlice', 'low high')):
    def __call__(self, statement, v):
        return slice(self.low(None, statement, v) - 1,
                     self.high(None, statement, v))


FieldSpec = namedlist('FieldSpec', 'expr alias')
OrderBy = namedlist('OrderBy', 'expr desc')


def is_expression(t):
    return isinstance(t, (Expression, UnaryExpression))
