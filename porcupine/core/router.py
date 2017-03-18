import logging
import time
from functools import wraps
from sanic.router import Router
from .context import with_context


class ContextRouter(Router):
    def get(self, request):
        handler, args, kwargs = super().get(request)
        return self.request_context(handler), args, kwargs

    @staticmethod
    def request_context(handler):

        @wraps(handler)
        async def request_wrapper(*args, **kwargs):
            request = args[0]
            now = time.time()
            handler_with_context = with_context('system')(handler)
            response = await handler_with_context(*args, **kwargs)
            logging.info('"{0} {1} HTTP/{2}" {3} {4} {5}'.format(
                request.method, request.url, request.version,
                response.status, len(response.body),
                time.time() - now))
            return response

        return request_wrapper
