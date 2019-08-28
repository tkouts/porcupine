from sly import Lexer, Parser
from porcupine.core.oql.tokens import *
from porcupine.core.oql.statements import Select


class ParseError(SyntaxError):
    def __init__(self, t):
        self.token = t


# noinspection PyUnresolvedReferences,PyUnboundLocalVariable
class OqlLexer(Lexer):
    tokens = {
        AND, OR, NOT,
        EQ, LE, LT, GE, GT, NE,
        FLOAT, INT, BOOLEAN, NULL, STRING,
        VAR,
        NAME,
        FUNCTION,
        # keywords
        SELECT, AS, FROM, WHERE, ORDER, BY, ASC, DESC, RANGE
    }

    keywords = {
        'select', 'as', 'from', 'where', 'order', 'by',
        'asc', 'desc', 'range',
        # logical operators
        'and', 'or', 'not'
    }

    functions = {'len', 'slice', 'hasattr', 'date'}

    literals = {'=', '+', '-', '*', '/', '(', ')', '.', ','}

    ignore = ' \t\r'

    EQ = r'=='
    LE = r'<='
    LT = r'<'
    GE = r'>='
    GT = r'>'
    NE = r'!='

    @_(r'"(?:[^"]|"")*"')
    def STRING(self, t):
        t.value = t.value[1:-1].replace('""', '"')
        return t

    @_(r'\d+\.\d+(?:e(\+|-)?(\d+))?')
    def FLOAT(self, t):
        t.value = float(t.value)
        return t

    @_(r'\d+')
    def INT(self, t):
        t.value = int(t.value)
        return t

    @_('true', 'false')
    def BOOLEAN(self, t):
        t.value = t.value == 'true'
        return t

    @_('null')
    def NULL(self, t):
        t.value = None
        return t

    @_(r'\$[A-Za-z_][\w]*')
    def VAR(self, t):
        t.value = t.value[1:]
        return t

    @_(r'[A-Za-z_][\w]*')
    def NAME(self, t):
        lower = t.value.lower()
        if lower in self.keywords:
            t.type = t.value.upper()
        elif lower in self.functions:
            t.type = 'FUNCTION'
            t.value = lower
        return t

    @_(r'\n+')
    def ignore_newline(self, t):
        self.lineno += len(t.value)

    @_(r"--[^\n]*")
    def ignore_comment(self, t):
        ...

    def error(self, t):
        raise ParseError(t)


# noinspection PyUnresolvedReferences
class OqlParser(Parser):
    tokens = OqlLexer.tokens

    precedence = (
        # ('left', 'UNION', 'INTERSECTION'),
        ('left', 'OR'),
        ('left', 'AND'),
        ('left', 'NOT'),
        # ('left', 'IN', 'BETWEEN'),
        ('nonassoc', 'EQ', 'GT', 'GE', 'LT', 'LE', 'NE'),
        ('left', '+', '-'),
        ('left', '*', '/'),
        # ('left', 'EXP'),
        ('right', 'UMINUS'),
    )

    # Oql script

    @_('statement')
    def oql_script(self, p):
        return [p.statement]

    @_('oql_script statement')
    def oql_script(self, p):
        p.oql_script.append(p.statement)
        return p.oql_script

    # statements

    @_('select_statement')
    def statement(self, p):
        return p.select_statement

    # SELECT statement

    @_('SELECT "*" FROM scope')
    def select_statement(self, p):
        return Select(p.scope, [])

    @_('SELECT select_list FROM scope')
    def select_statement(self, p):
        return Select(p.scope, p.select_list)

    @_('select_statement WHERE expr')
    def select_statement(self, p):
        p.select_statement.where_condition = p.expr
        return p.select_statement

    @_('select_statement ORDER BY expr')
    def select_statement(self, p):
        p.select_statement.order_by = OrderBy(p.expr, False)
        return p.select_statement

    @_('select_statement ORDER BY expr ASC',
       'select_statement ORDER BY expr DESC')
    def select_statement(self, p):
        p.select_statement.order_by = OrderBy(p.expr, p[4] == 'desc')
        return p.select_statement

    @_('select_statement RANGE INT "-" INT')
    def select_statement(self, p):
        p.select_statement.range = slice(p.INT0 - 1, p.INT1)
        return p.select_statement

    # field spec

    @_('expr')
    def field_spec(self, p):
        return FieldSpec(p.expr, None)

    @_('field_spec AS NAME')
    def field_spec(self, p):
        p.field_spec.alias = p.NAME
        return p.field_spec

    # select list

    @_('field_spec')
    def select_list(self, p):
        return [p.field_spec]

    @_('select_list "," field_spec')
    def select_list(self, p):
        p.select_list.append(p.field_spec)
        return p.select_list

    # scope

    @_('NAME')
    def scope(self, p):
        return Scope(p.NAME, 'children')

    @_('VAR')
    def scope(self, p):
        return Scope(Variable(p.VAR), 'children')

    @_('scope "." NAME')
    def scope(self, p):
        p.scope.collection = p.NAME
        return p.scope

    # expression list

    @_('expr')
    def expression_list(self, p):
        return [p.expr]

    @_('expression_list "," expr')
    def expression_list(self, p):
        p.expression_list.append(p.expr)
        return p.expression_list

    # expression

    @_(
        # arithmetic
        'expr "+" expr', 'expr "-" expr', 'expr "*" expr', 'expr "/" expr',
        # comparison
        'expr EQ expr', 'expr NE expr', 'expr GT expr', 'expr GE expr',
        'expr LT expr', 'expr LE expr',
        # logical
        'expr AND expr', 'expr OR expr'
    )
    def expr(self, p):
        return Expression(p.expr0, p[1], p.expr1)

    @_('NOT expr')
    def expr(self, p):
        return UnaryExpression('not', p.expr)

    @_('"-" expr %prec UMINUS')
    def expr(self, p):
        return UnaryExpression('-', p.expr)

    @_('FUNCTION "(" expression_list ")"')
    def expr(self, p):
        return FunctionCall(p.FUNCTION, p.expression_list)

    @_('"(" expr ")"')
    def expr(self, p):
        return p.expr

    @_('VAR')
    def expr(self, p):
        return Variable(p.VAR)

    @_('BOOLEAN')
    def expr(self, p):
        return T_False if p.BOOLEAN else T_False

    @_('NULL')
    def expr(self, _):
        return T_Null

    @_('STRING')
    def expr(self, p):
        return String(p.STRING)

    @_('NAME')
    def expr(self, p):
        return Field(p.NAME)

    @_('INT')
    def expr(self, p):
        return Integer(p.INT)

    @_('FLOAT')
    def expr(self, p):
        return Float(p.FLOAT)

    def error(self, p):
        raise ParseError(p)


if __name__ == '__main__':
    lexer = OqlLexer()
    parser = OqlParser()
    tokens = lexer.tokenize(
        'select x + 1, $y from $x1.items where x order by x desc')
    print([x for x in tokens])
    # print(parser.parse(tokens))
