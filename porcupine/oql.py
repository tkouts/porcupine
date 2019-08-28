import inspect
from lru import LRU
from porcupine.core.oql import parse, prepare
# from porcupine.exceptions import OqlSyntaxError


_query_cache = LRU(100)


async def execute(script, variables=None):
    if variables is None:
        variables = {}
    if script in _query_cache:
        prepared = _query_cache[script]
    else:
        ast = parse(script)
        prepared = prepare(ast)
        _query_cache[script] = prepared

    output = []
    for statement in prepared:
        result = statement.execute(variables)
        if inspect.isawaitable(result):
            result = await result
        if result is not None:
            output.append(result)

    if len(output) == 1:
        output = output[0]

    return output
