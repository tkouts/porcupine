from operator import itemgetter

from porcupine.exceptions import OqlError
from porcupine.core.utils import date


def get_field(item, field, statement, var_map):
    if field in statement.computed_fields:
        return statement.computed_fields[field](item, statement, var_map)
    return getattr(item, field)


def get_nested_field(item, field, path):
    attr = getattr(item, field)
    for key in path:
        attr = attr[key]
    return attr


def get_var(var_map, var_name):
    try:
        return var_map[var_name]
    except KeyError:
        raise OqlError(f'Unknown variable "{var_name}"')


environment = {
    # functions
    'len': lambda i, x: len(x),
    'slice': lambda i, x, start, end: x[start:end],
    'hasattr': hasattr,
    'datetime': lambda i, x: date.get(x),
    'date': lambda i, x: date.get(x, date_only=True),

    # helpers
    'get_nested_field': get_nested_field,
    'get_field': get_field,
    'get_var': get_var
}
