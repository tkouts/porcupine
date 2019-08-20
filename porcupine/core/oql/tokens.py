from namedlist import namedlist


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
    'FieldSpec',
    'Scope',
    'is_expression'
)


class Token:
    immutable = True

    def __init__(self, value, *_args, **_kwargs):
        self.value = value

    def __repr__(self):
        return f'{self.__class__.__name__}({str(self.value)})'

    def compile(self):
        code = f'lambda i, s, v: {self.source()}'
        # print(code)
        return eval(code)

    def source(self):
        return str(self.value)


T_True = Token(True)
T_False = Token(False)
T_Null = Token(None)


class Integer(Token, int):
    def source(self):
        return self


class Float(Token, float):
    def source(self):
        return self


class String(Token, str):
    def source(self):
        return f'"{self}"'


class Field(Token, str):
    immutable = False

    def source(self):
        return f's.get_field(i, "{self}", v)'


class Variable(Token, str):
    def source(self):
        return f's.get_var(v, "{self}")'


class FunctionCall(namedlist('FunctionCall', 'func, args'), Token):
    immutable = False

    def source(self):
        args = [f'"{self.func}"', 'i']
        if self.args:
            args.extend([arg.source() for arg in self.args])
        return f's.call_func({", ".join(args)})'


class Expression(namedlist('Expression', 'l_op operator r_op'), Token):
    def source(self):
        return f'({self.l_op.source()} {self.operator} {self.r_op.source()})'


class UnaryExpression(namedlist('UnaryExpression', 'operator operand'), Token):
    def source(self):
        spacer = ' ' if self.operator == 'not' else ''
        return f'{self.operator}{spacer}{self.operand.source()}'


FieldSpec = namedlist('FieldSpec', 'expr alias')
Scope = namedlist('Scope', 'item_id collection')


def is_expression(t):
    return isinstance(t, (Expression, UnaryExpression))
