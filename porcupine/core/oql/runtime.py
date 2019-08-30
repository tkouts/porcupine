from functools import lru_cache

from porcupine.exceptions import OqlError
from porcupine.core.utils import date


def get_field(item, field, statement, var_map):
    if field in statement.computed_fields:
        return statement.computed_fields[field](item, statement, var_map)
    return getattr(item, field)


def get_var(var_map, var_name):
    try:
        return var_map[var_name]
    except KeyError:
        raise OqlError(f'Unknown variable "{var_name}"')


@lru_cache(maxsize=100, typed=False)
def get_date(x):
    return date.get(x)


environment = {
    # functions
    'len': lambda i, x: len(x),
    'slice': lambda i, x, start, end: x[start:end],
    'hasattr': hasattr,
    'date': lambda i, x: get_date(x),

    # helpers
    'get_field': get_field,
    'get_var': get_var
}
