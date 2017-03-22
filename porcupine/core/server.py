from sanic import Sanic
from .router import ContextRouter

server = Sanic(router=ContextRouter())
