from porcupine.core.utils.collections import WriteOnceDict

_ELASTIC_MAP = WriteOnceDict()
_INDEXES = {}
_FULL_TEST_INDEXES = {}


def register(cls):
    _ELASTIC_MAP[cls.__name__] = cls


def get_content_class(name: str):
    return _ELASTIC_MAP[name]


def get_many_to_many_relationships():
    from porcupine.core.datatypes.relator import RelatorN
    rels = []
    for cls in _ELASTIC_MAP.values():
        for dt in cls.__dict__.values():
            if isinstance(dt, RelatorN) and dt.is_many_to_many:
                rels.append(dt)
    return rels


def get_compositions():
    from porcupine.core.datatypes.composition import (
        Composition,
        Embedded
    )
    comp_types = Composition, Embedded
    comps = []
    for cls in _ELASTIC_MAP.values():
        for dt in cls.__dict__.values():
            if isinstance(dt, comp_types):
                # print(dt.allowed_types)
                for composite_class in dt.allowed_types:
                    composite_class.embedded_in = cls
                    composite_class.collection_name = dt.name
                comps.append((cls, dt))
    return comps


def add_indexes(cls, indexes):
    _INDEXES[cls] = indexes


def add_fts_indexes(cls, fts_indexes):
    _FULL_TEST_INDEXES[cls] = fts_indexes
