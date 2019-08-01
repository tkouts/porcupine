"""
Abstract Service class
"""


class AbstractService:
    service_key = None
    priority = 0

    def __init__(self, server):
        self.server = server

    def start(self, loop):
        raise NotImplementedError

    def stop(self, loop):
        raise NotImplementedError

    async def status(self):
        return {}
