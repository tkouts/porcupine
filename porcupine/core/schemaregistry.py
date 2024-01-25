from porcupine.core.utils.collections import WriteOnceDict


_ELASTIC_MAP = WriteOnceDict()
_INDEXES = {}
_FULL_TEST_INDEXES = {}


def register(cls):
    _ELASTIC_MAP[cls.__name__] = cls


def get_content_class(name: str):
    return _ELASTIC_MAP[name]
