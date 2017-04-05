"""
Abstract Service class
"""


class AbstractService:
    @classmethod
    def start(cls, server):
        raise NotImplementedError

    @classmethod
    def stop(cls, server):
        raise NotImplementedError

    @classmethod
    def status(cls):
        return {}
