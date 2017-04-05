from sanic import Sanic
from sanic.request import Request
from .router import ContextRouter

# add session to request
Request.session = property(lambda req: req.get('session'))

server = Sanic(router=ContextRouter())
