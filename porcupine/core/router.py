from sanic.router import Router
from .context import with_context


class ContextRouter(Router):
    def __init__(self):
        super().__init__()
        self.server = None

    def get(self, request):
        handler, args, kwargs, uri = super().get(request)
        return with_context(identity='system', debug=self.server.debug)(handler), args, kwargs, uri
