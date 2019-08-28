from porcupine.core.oql.parser import OqlLexer, OqlParser, ParseError
from porcupine.core.oql.statements import Select
from porcupine.exceptions import OqlSyntaxError


def parse(script):
    oql_lexer = OqlLexer()
    oql_parser = OqlParser()
    try:
        return oql_parser.parse(oql_lexer.tokenize(script))
    except ParseError as pe:
        token = pe.token
        script_lines = script.split('\n')

        if token is None:
            line_no = len(script_lines) - 1
            msg = 'Unexpected end of OQL script'
            column = len(script_lines[line_no]) - 1
        else:
            line_no = pe.token.lineno - 1
            column = pe.token.index
            err_value = pe.token.value[0]
            msg = f'OQL syntax error at line {line_no + 1}: "{err_value}"'

        helper_string = f'{script_lines[line_no]}\n{" " * column + "^"}'
        error_string = f'\n{helper_string}\n{msg}'
        raise OqlSyntaxError(error_string)


def prepare(ast):
    prepared_ast = []
    for statement in ast:
        statement.prepare()
        prepared_ast.append(statement)
    return prepared_ast


if __name__ == '__main__':
    # now = time.time()
    # oql_script.setDebug(True)
    try:
        parsed = parse('select not id_4d + 1 or 1.0 > 2, name + "a", true from ROOT.podsf')
        prepare(parsed)
    except OqlSyntaxError as se:
        print(se.line)
        print(' ' * (se.col - 1) + '^')

    # print(time.time() - now)
