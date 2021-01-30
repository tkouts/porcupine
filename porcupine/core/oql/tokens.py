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

    def __repr__(self):
        return f'{self.__class__.__name__}({self.value})'

    def compile(self) -> Callable:
        code = f'lambda i, s, v: {self.source()}'
        return eval(code, environment)

    def source(self):
        return repr(self.value)

    def _get_indexed_container_type(self, container_type) -> None:
        return None

    def get_index_lookup(self, container_type, **options) -> None:
        return None

    def optimize(self, container_type, **options):
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

    def source(self):
        if '.' in self:
            # nested attribute
            path = self.split('.')
            return f'get_nested_field(i, "{path[0]}", {path[1:]})'
        return f'get_field(i, "{self}", s, v)'

    @lru_cache(maxsize=None)
    def _get_indexed_container_type(self, container_type) -> Optional[type]:
        for cls in container_type.mro():
            if 'indexes' in cls.__dict__ and self in cls.indexes:
                return cls
        return None

    def get_index_lookup(self, container_type,
                         **options) -> Optional[IndexLookup]:
        indexed_type = self._get_indexed_container_type(container_type)
        if indexed_type is not None:
            index_lookup = IndexLookup(indexed_type, self, options=options)
            return index_lookup

    def optimize(self, container_type, **options):
        return self.get_index_lookup(
            container_type,
            **options
        )


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

    def get_index_lookup(self, container_type, **options):
        indexed_type = self._get_indexed_container_type(container_type)
        if indexed_type is not None:
            index_lookup = FTSIndexLookup(
                indexed_type,
                self.field,
                self.term,
                options=options
            )
            return index_lookup
        log.warn(f'Non indexed FTS lookup on {container_type.__name__}')
        return EmptyFeeder()

    def optimize(self, container_type, **options):
        if not self.term.immutable:
            raise OqlError('FREETEXT term should be immutable')
        return self.get_index_lookup(
            container_type,
            **options
        )


class Expression(namedlist('Expression', 'l_op operator r_op'), Token):
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

    def optimize(self, container_type, **options):
        # comparison
        is_comparison = self.operator in {'==', '>=', '<=', '>', '<'}
        if is_comparison:
            operator = self.operator
            bounds = None
            index_lookup = None
            if self.r_op.immutable:
                index_lookup = self.l_op.get_index_lookup(
                    container_type,
                    **options
                )
                bounds = self.r_op
            elif self.l_op.immutable:
                # reverse operator
                if operator.startswith('>'):
                    operator = f'<{operator[1:]}'
                elif operator.startswith('<'):
                    operator = f'>{operator[1:]}'
                index_lookup = self.r_op.get_index_lookup(
                    container_type,
                    **options
                )
                bounds = self.l_op

            if index_lookup is not None:
                bounds = bounds.compile()
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
                return index_lookup

        # logical
        is_logical = self.operator in {'and', 'or'}
        if is_logical:
            optimized = [op.optimize(container_type, **options)
                         for op in (self.l_op, self.r_op)]
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
