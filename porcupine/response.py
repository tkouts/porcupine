from sanic.response import *


def no_content(headers=None):
    return HTTPResponse('', status=204, headers=headers)
