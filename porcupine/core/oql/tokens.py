from typing import Optional, Callable
from namedlist import namedlist
from functools import lru_cache

# from porcupine.core.services import db_connector
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

    def __repr__(self):
        return f'{self.__class__.__name__}({self.value})'

    def compile(self) -> Callable:
        code = f'lambda i, s, v: {self.source()}'
        # print(code)
        return eval(code, environment)

    def source(self):
        return repr(self.value)

    def get_index_type(self, container_type) -> Optional[type]:
        return None

    def get_index_lookup(self,
                         container_type,
                         operator: Optional[str] = None,
                         bounds: Optional[Callable] = None) -> None:
        return None


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

    @staticmethod
    def optimize():
        return None

    def source(self):
        if '.' in self:
            # nested attribute
            path = self.split('.')
            return f'get_nested_field(i, "{path[0]}", {path[1:]})'
        return f'get_field(i, "{self}", s, v)'

    @lru_cache(maxsize=None)
    def get_index_type(self, container_type) -> Optional[type]:
        for cls in container_type.mro():
            if 'indexes' in cls.__dict__ and self in cls.indexes:
                return cls
        return None

    def get_index_lookup(self,
                         index_type,
                         operator: Optional[str] = None,
                         bounds: Optional[Callable] = None,
                         **options) -> IndexLookup:
        index_lookup = IndexLookup(index_type, self, options=options)
        if operator is not None:
            if operator == '==':
                # equality
                index_lookup.bounds = bounds
            elif operator.startswith('>'):
                dynamic_range = DynamicRange(bounds)
                dynamic_range.l_inclusive = operator == '>='
                index_lookup.bounds = dynamic_range
            elif operator.startswith('<'):
                dynamic_range = DynamicRange(None, False, bounds)
                dynamic_range.u_inclusive = operator == '<='
                index_lookup.bounds = dynamic_range
        else:
            # scan all
            index_lookup.bounds = DynamicRange()
        return index_lookup


class Variable(Token, str):
    primitive = False

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'

    def source(self):
        return f'get_var(v, "{self}")'


class FunctionCall(namedlist('FunctionCall', 'func, args'), Token):
    primitive = False

    @property
    def immutable(self):
        return all((arg.immutable for arg in self.args))

    def source(self):
        args = ['i']
        if self.args:
            args.extend([arg.source() for arg in self.args])
        return f'{self.func}({", ".join(args)})'


class Expression(namedlist('Expression', 'l_op operator r_op'), Token):
    @property
    def primitive(self):
        return self.l_op.primitive and self.r_op.primitive

    @property
    # @lru_cache(maxsize=None)
    def immutable(self):
        return self.l_op.immutable and self.r_op.immutable

    def source(self):
        source = f'{self.l_op.source()} {self.operator} {self.r_op.source()}'
        if self.primitive:
            return repr(eval(source))
        return source

    # def __hash__(self):
    #     return id(self)

    def optimize(self, container_type, **options):
        # comparison
        is_comparison = self.operator in {'==', '>=', '<=', '>', '<'}
        if is_comparison:
            if self.r_op.immutable:
                index_type = self.l_op.get_index_type(container_type)
                if index_type is not None:
                    bounds = self.r_op.compile()
                    return self.l_op.get_index_lookup(
                        index_type,
                        self.operator,
                        bounds,
                        **options
                    )
            if self.l_op.immutable:
                index_type = self.r_op.get_index_type(container_type)
                if index_type is not None:
                    bounds = self.l_op.compile()
                    # reverse operator
                    operator = self.operator
                    if operator.startswith('>'):
                        operator = f'<{operator[1:]}'
                    elif operator.startswith('<'):
                        operator = f'>{operator[1:]}'
                    return self.r_op.get_index_lookup(
                        index_type,
                        operator,
                        bounds,
                        **options
                    )

        # logical
        is_logical = self.operator in {'and', 'or'}
        if is_logical:
            optimized = [op.optimize() for op in (self.l_op, self.r_op)]
            if self.operator == 'and':
                if all(optimized):
                    # intersection
                    return Intersection(*optimized)
                elif any(optimized):
                    if optimized[0] is not None:
                        feeder = optimized[0]
                        flt = self.r_op.compile()
                    else:
                        feeder = optimized[1]
                        flt = self.l_op.compile()
                    feeder.filter_func = flt
                    return feeder
            if self.operator == 'or':
                if all(optimized):
                    # union
                    return Union(*optimized)


class UnaryExpression(namedlist('UnaryExpression', 'operator operand'), Token):
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
