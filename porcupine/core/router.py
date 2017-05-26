from sanic.router import Router
from .context import with_context


class ContextRouter(Router):
    def get(self, request):
        handler, args, kwargs, uri = super().get(request)
        return with_context()(handler), args, kwargs, uri
