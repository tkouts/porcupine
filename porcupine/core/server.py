from sanic import Sanic
from .log import setup_logging
from .router import ContextRouter

setup_logging()
server = Sanic(router=ContextRouter())
