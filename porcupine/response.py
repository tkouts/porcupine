from orjson import dumps
from sanic.response import *

from porcupine.core.utils import default_json_encoder


def no_content(headers=None):
    return HTTPResponse('', status=204, headers=headers)


def json(body, status=200, headers=None,
         content_type='application/json',
         default=default_json_encoder,
         **kwargs):
    return HTTPResponse(
        body_bytes=dumps(body, default=default, **kwargs),
        headers=headers,
        status=status,
        content_type=content_type,
    )
