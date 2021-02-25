from typing import Optional, Callable
from namedlist import namedlist
from functools import lru_cache

from porcupine import log
from porcupine.exceptions import OqlError
from porcupine.core.oql.runtime import environment
from porcupine.core.oql.feeder import *
from porcupine.core.oql.feederproxy import Argument, FeederProxy

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

    @property
    def field_lookups(self):
        return []

    def __repr__(self):
        return f'{self.__class__.__name__}({self.value})'

    def compile(self) -> Callable:
        if self.__lambda is None:
            code = f'lambda i, s, v: {self.source()}'
            self.__lambda = eval(code, environment)
        return self.__lambda

    def source(self):
        return repr(self.value)

    def get_index_lookup(self, container_type) -> Optional[FeederProxy]:
        return None

    def optimize(self, item_type, **options) -> Optional[Feeder]:
        feeder_proxy = self.get_index_lookup(item_type)
        if feeder_proxy is not None:
            return feeder_proxy.value(options)
        return None

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

    @property
    def field_lookups(self):
        return [self]

    def source(self):
        if '.' in self:
            # nested attribute
            path = self.split('.')
            return f'get_nested_field(i, "{path[0]}", {path[1:]})'
        return f'get_field(i, "{self}", s, v)'

    @lru_cache(maxsize=None)
    def get_index_lookup(self, container_type) -> Optional[FeederProxy]:
        for cls in container_type.mro():
            if 'indexes' in cls.__dict__:
                for attr_set in cls.indexes:
                    if attr_set == self:
                        return FeederProxy(IndexLookup,
                                           index_type=cls,
                                           index_name=self)
                        # return cls, attr_set
                    elif isinstance(attr_set, list) and attr_set[0] == self:
                        return FeederProxy(IndexLookup,
                                           index_type=cls,
                                           index_name=','.join(attr_set))
        return None


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
    def get_index_lookup(self, container_type) -> Optional[FeederProxy]:
        for cls in container_type.mro():
            if 'full_text_indexes' in cls.__dict__:
                if self.field == '*' or self in cls.full_text_indexes:
                    return FeederProxy(FTSIndexLookup,
                                       index_type=cls,
                                       field=self.field,
                                       term=self.term)
        return None

    def optimize(self, item_type, **options) -> Feeder:
        if not self.term.immutable:
            raise OqlError('FREETEXT term should be immutable')
        feeder = super().optimize(item_type, **options)
        if feeder is not None:
            return feeder
        log.warn(f'Non indexed FTS lookup on {item_type}')
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

    @property
    def field_lookups(self):
        if self.operator in {'==', '>=', '<=', '>', '<', 'and'}:  # or any([isinstance(op, Field) for x in]):
        #     return [op for op in (self.r_op, self.l_op)
        #             if isinstance(op, Field)]
            return self.r_op.field_lookups + self.l_op.field_lookups
        return []

    def source(self):
        source = f'{self.l_op.source()} {self.operator} {self.r_op.source()}'
        if self.primitive:
            return repr(eval(source))
        return source

    def __hash__(self):
        return hash(self.source())

    def __call__(self, statement, v):
        return self.compile()(None, statement, v)

    @lru_cache(maxsize=None)
    def get_index_lookup(self, container_type) -> Optional[FeederProxy]:
        # comparison
        is_comparison = self.operator in {'==', '>=', '<=', '>', '<'}
        if is_comparison:
            operator = self.operator
            bounds = None
            feeder_proxy = None
            if self.r_op.immutable:
                feeder_proxy = self.l_op.get_index_lookup(container_type)
                bounds = self.r_op
            elif self.l_op.immutable:
                # reverse operator
                if operator.startswith('>'):
                    operator = f'<{operator[1:]}'
                elif operator.startswith('<'):
                    operator = f'>{operator[1:]}'
                feeder_proxy = self.r_op.get_index_lookup(container_type)
                bounds = self.l_op

            if feeder_proxy is not None:
                bounds = bounds.compile()
                if operator.startswith('>'):
                    bounds = DynamicRange(bounds)
                    bounds.l_inclusive = operator == '>='
                elif operator.startswith('<'):
                    bounds = DynamicRange(None, False, bounds)
                    bounds.u_inclusive = operator == '<='
                feeder_proxy.set_argument('bounds', bounds)
                return feeder_proxy

        # logical
        is_logical = self.operator in {'and', 'or'}
        if is_logical:
            optimized = [
                OptimizedExpr(op, op.get_index_lookup(container_type))
                for op in (self.l_op, self.r_op)
            ]
            optimized.sort(
                key=lambda f: 1 if f.feeder is None else -f.feeder.priority
            )
            first, second = optimized
            if self.operator == 'and':
                print(self.field_lookups)
                # print(optimized)
                if first.feeder is not None:
                    if second.feeder is not None:
                        # intersection
                        return FeederProxy(
                            Intersection,
                            first=first.feeder,
                            second=second.feeder
                        )
                    else:
                        # add filter func
                        first.feeder.set_argument('filter_func',
                                                  second.expr.compile())
                        return first.feeder
            if self.operator == 'or':
                # print(optimized)
                if first.feeder is not None:
                    # union
                    if second.feeder is None:
                        second.feeder = FeederProxy(
                            CollectionFeeder,
                            filter_func=second.expr.compile
                        )
                    return FeederProxy(
                        Union,
                        first=first.feeder,
                        second=second.feeder
                    )
        return None


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
