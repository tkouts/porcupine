"""
Abstract Service class
"""


class AbstractService:
    @classmethod
    def start(cls):
        raise NotImplementedError

    @classmethod
    def stop(cls):
        raise NotImplementedError
