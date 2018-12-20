"""
Abstract Service class
"""


class AbstractService:
    service_key = None

    def __init__(self, server):
        self.server = server

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def status(self):
        return {}
