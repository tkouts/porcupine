from methodtools import lru_cache
from porcupine.core.utils.collections import WriteOnceDict

_ELASTIC_MAP = WriteOnceDict()


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


def get_compositions(root_cls=None):
    from porcupine.core.datatypes.composition import (
        Composition,
        Embedded
    )
    from porcupine.core.schema.item import GenericItem
    comp_types = Composition, Embedded
    comps = []
    # print(root_cls)
    for cls in get_all_subclasses(root_cls or GenericItem)[1:]:
        for dt in cls.__dict__.values():
            if isinstance(dt, comp_types):
                # print(dt.allowed_types)
                for composite_class in dt.allowed_types:
                    composite_class.embedded_in = cls
                    composite_class.collection_name = dt.name
                comps.append((cls, dt))
                comps.extend(get_compositions(cls))
    return comps


# def get_unique_constraints():
#     # from porcupine.core.datatypes.datatype import DataType
#     uniques = []
#     for cls in _ELASTIC_MAP.values():
#         # for dt in cls.__dict__.values():
#         if cls.is_collection and 'unique_constraints' in cls.__dict__:
#             for attr in cls.unique_constraints:
#                 uniques.append((
#                     cls,
#                     attr,
#                     [
#                         cls.__name__
#                         for cls in get_all_subclasses(cls)
#                         if attr in cls.unique_constraints
#                     ]
#                 ))
#     return uniques


def get_indexes():
    from porcupine.core.datatypes.relator import RelatorN
    indexes = {}
    for cls in _ELASTIC_MAP.values():
        for dt in cls.__schema__.values():
            if (
                isinstance(dt, RelatorN)
                and not dt.is_many_to_many
            ):
                dt = getattr(cls, dt.name)
                if dt.indexes:
                    for index in dt.indexes:
                        indexes.setdefault(index, {'dt': dt, 'cls': []})
                        indexes[index]['cls'].append(cls)
    return indexes


def get_fts_indexes():
    fts_indexes = []
    for cls in _ELASTIC_MAP.values():
        if cls.is_collection and 'full_text_indexes' in cls.__dict__:
            fts_indexes.append((
                cls,
                cls.full_text_indexes,
                [cls.__name__ for cls in get_all_subclasses(cls)]
            ))
    return fts_indexes


def get_all_subclasses(cls, condition=None):
    subclasses = [cls]
    for subclass in cls.__subclasses__():
        if condition and not condition(subclass):
            continue
        subclasses.extend(get_all_subclasses(subclass))
    return subclasses


@lru_cache(maxsize=None)
def get_datatype_from_attr_name(classes, name):
    for cls in classes:
        if name in cls.__schema__:
            return cls.__schema__[name]
        key = get_datatype_from_attr_name(tuple(cls.__subclasses__()), name)
        if key:
            return key
    raise NameError(f"Unknown schema attribute '{name}'.")
