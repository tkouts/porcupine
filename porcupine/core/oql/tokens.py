from typing import Optional, Callable
from functools import lru_cache, partial
from namedlist import namedlist

from porcupine.exceptions import OqlError
from porcupine.core.oql.runtime import environment
from porcupine.core.oql.feeder import *
from porcupine.core.oql.optimizers import (
    FieldLookup,
    fts_token_optimizer,
    conjunction_optimizer,
    disjunction_optimizer,
)

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
    is_field_lookup = False

    def __init__(self, value, *_args, **_kwargs):
        self.value = value
        self.__lambda = None

    @property
    def field_lookup(self) -> Optional[FieldLookup]:
        return None

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.value)})'

    def compile(self) -> Callable:
        if self.__lambda is None:
            code = f'lambda i, s, v: {self.source()}'
            self.__lambda = eval(code, environment)
        return self.__lambda

    def source(self):
        return repr(self.value)

    def get_index_lookup(self, container_type, order_by) -> Optional[partial]:
        return None

    def optimize(self, item_type, order_by, **options) -> Optional[Feeder]:
        feeder_proxy = self.get_index_lookup(item_type, order_by)
        if feeder_proxy is not None:
            return feeder_proxy(options=options)
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
    is_field_lookup = True

    @property
    def field_lookup(self) -> Optional[FieldLookup]:
        return FieldLookup(self)

    def source(self):
        if '.' in self:
            # nested attribute
            path = self.split('.')
            return f'get_nested_field(i, "{path[0]}", {path[1:]})'
        return f'get_field(i, "{self}", s, v)'

    @lru_cache(maxsize=1024)
    def get_index_lookup(self, container_type, order_by) -> Optional[partial]:
        return conjunction_optimizer([self], container_type, order_by)


class Variable(Token, str):
    primitive = False

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'

    def source(self):
        return f'get_var(v, {self.split(".")})'

    def __call__(self, statement, v):
        return environment['get_var'](v, self.split('.'))


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


class FreeText(namedlist('FreeText', 'term field type'), Token):
    primitive = False
    immutable = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Token.__init__(self, True)

    def __hash__(self):
        return hash(self.field)

    @lru_cache(maxsize=1024)
    def get_index_lookup(self, container_type, order_by) -> partial:
        return fts_token_optimizer(self, container_type)

    def optimize(self, item_type, order_by, **options) -> Feeder:
        if not self.term.immutable:
            raise OqlError('FREETEXT term should be immutable')
        return super().optimize(item_type, order_by, **options)


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
    def is_field_lookup(self):
        if self.is_comparison and (self.r_op.immutable or self.l_op.immutable):
            return self.r_op.is_field_lookup or self.l_op.is_field_lookup

    @property
    def operands(self):
        return self.l_op, self.r_op

    def explode(self):
        tokens = []
        for operand in self.operands:
            if isinstance(operand, Expression) and \
                    operand.operator == self.operator:
                tokens.extend(operand.explode())
            else:
                tokens.append(operand)
        return tokens

    @property
    def field_lookup(self) -> Optional[FieldLookup]:
        if self.is_field_lookup:
            if self.r_op.immutable:
                field_lookup = self.l_op.field_lookup
                field_lookup.operator = self.operator
                field_lookup.expr = self.r_op
                return field_lookup
            elif self.l_op.immutable:
                field_lookup = self.r_op.field_lookup
                operator = self.operator
                # reverse operator
                if operator.startswith('>'):
                    operator = f'<{operator[1:]}'
                elif operator.startswith('<'):
                    operator = f'>{operator[1:]}'
                field_lookup.operator = operator
                field_lookup.expr = self.l_op
                return field_lookup

    @property
    def is_comparison(self):
        return self.operator in {'==', '>=', '<=', '>', '<'}

    @property
    def is_logical(self):
        return self.operator in {'and', 'or'}

    def source(self):
        source = f'{self.l_op.source()} {self.operator} {self.r_op.source()}'
        if self.primitive:
            return repr(eval(source))
        return source

    def __hash__(self):
        return hash(self.source())

    def __call__(self, statement, v):
        return self.compile()(None, statement, v)

    @lru_cache(maxsize=1024)
    def get_index_lookup(self, container_type, order_by) -> Optional[partial]:
        # field lookup
        if self.is_field_lookup:
            return conjunction_optimizer([self], container_type, order_by)

        # logical
        if self.is_logical:
            tokens = self.explode()
            if self.operator == 'and':
                return conjunction_optimizer(tokens, container_type, order_by)
            if self.operator == 'or':
                return disjunction_optimizer(tokens, container_type, order_by)


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


class OrderBy(namedlist('OrderBy', 'expr desc')):
    @property
    def fields(self) -> Optional[tuple]:
        if isinstance(self.expr, Field):
            return (self.expr, )
        return None


def is_expression(t):
    return isinstance(t, (Expression, UnaryExpression))
