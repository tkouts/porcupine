from sanic.router import Router
from .context import with_context


class ContextRouter(Router):
    def __init__(self):
        super().__init__()
        self.server = None

    def get(self, request):
        handler, args, kwargs, uri = super().get(request)
        identity = None
        session = request.session
        if session is not None and session['uid'] is not None:
            identity = session['uid']
        context_handler = with_context(
            identity=identity, debug=self.server.debug)(handler)
        return context_handler, args, kwargs, uri
