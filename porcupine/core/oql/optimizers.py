import itertools
from collections import defaultdict
from functools import lru_cache, partial
from typing import Optional

from porcupine import log
from porcupine.core.oql.runtime import environment
from porcupine.core.oql.feederproxy import FeederProxy
from porcupine.core.oql.feeder import (
    EmptyFeeder,
    IndexLookup,
    FTSIndexLookup,
    Intersection,
    Union,
    CollectionFeeder
)
from porcupine.core.oql.boundproxies import (
    BoundProxyBase,
    FixedBoundaryProxy,
    RangedBoundaryProxy,
)
from porcupine.core.services import db_connector


class FieldLookup:
    __slots__ = 'field', 'operator', 'expr'

    def __init__(self, field, operator=None, expr=None):
        self.field = field
        self.operator = operator
        self.expr = expr

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'field={repr(self.field)}, '
            f'operator={repr(self.operator)}, '
            f'expr={repr(self.expr)}'
            ')'
        )

    def get_bounds(self) -> BoundProxyBase:
        if self.expr is None:
            # scan all
            return RangedBoundaryProxy()
        bounds = self.expr.compile()
        operator = self.operator
        if operator.startswith('>'):
            bounds = RangedBoundaryProxy(l_bound=bounds)
            bounds.l_inclusive = operator == '>='
        elif operator.startswith('<'):
            bounds = RangedBoundaryProxy(u_bound=bounds)
            bounds.u_inclusive = operator == '<='
        else:
            bounds = FixedBoundaryProxy(bounds)
        return bounds


@lru_cache(maxsize=128)
def get_all_indexes(container_type) -> tuple:
    indexes = []
    db = db_connector()
    for cls in container_type.mro():
        if 'indexes' in cls.__dict__:
            indexes.extend(db.views[cls].values())
    return tuple(indexes)


@lru_cache(maxsize=128)
def get_all_fts_indexes(container_type) -> tuple:
    fts_indexes = []
    db = db_connector()
    for cls in container_type.mro():
        if 'full_text_indexes' in cls.__dict__:
            fts_indexes.append(db.fts_indexes[cls])
    return tuple(fts_indexes)


def fts_token_optimizer(token, container_type) -> partial:
    available_indexes = []
    for index in get_all_fts_indexes(container_type):
        if token.field == '*' or token.field in index.attr_list:
            available_indexes.append(index)
            if token.field != '*':
                # one is enough if field is in index
                break
    if available_indexes:
        feeder_proxies = [
            FeederProxy.factory(FTSIndexLookup, index, token.field, token.term)
            for index in available_indexes
        ]
        if len(feeder_proxies) == 1:
            return feeder_proxies[0]
        else:
            return FeederProxy.factory(
                Union,
                *feeder_proxies
            )
    log.warn(f'Non indexed FTS lookup on {container_type.__name__}')
    return FeederProxy.factory(EmptyFeeder)


def conjunction_optimizer(tokens,
                          container_type, order_by) -> Optional[partial]:
    field_lookups = []
    rest = []
    for token in tokens:
        field_lookup = token.field_lookup
        if field_lookup is not None:
            field_lookups.append(field_lookup)
        else:
            rest.append(token)

    fields_map = defaultdict(list)
    for lookup in field_lookups:
        fields_map[lookup.field].append(lookup)
    field_names = fields_map.keys()
    indexes = get_all_indexes(container_type)

    # find index matches
    index_matches = []
    for index in indexes:
        match_length = min(len(fields_map), len(index.attr_list))
        match_fields = index.attr_list[:match_length]
        for mutation in itertools.permutations(field_names,
                                               match_length):
            partial_match = []
            for i, field in enumerate(match_fields):
                if mutation[i] == field:
                    partial_match.append(field)
            if partial_match:
                index_matches.append((partial_match, index))

    feeder_proxies = []
    if index_matches:
        index_matches.sort(key=lambda m: -len(m[0]))
        for match, index in index_matches:
            if all([f in fields_map for f in match]):
                lookups = [fields_map[f] for f in match]

                # check if we can query index
                valid_field_lookups = []
                if len(lookups) > 1:
                    for field_lookups in lookups:
                        valid_field_lookups.append(field_lookups)
                        is_valid = all([lu.operator == '=='
                                        for lu in field_lookups])
                        if not is_valid:
                            break
                else:
                    valid_field_lookups = lookups

                if valid_field_lookups:
                    bounds = []
                    for field_lookups in valid_field_lookups:
                        boundary = field_lookups[0].get_bounds()
                        for lookup in field_lookups[1:]:
                            boundary = boundary.intersection(
                                lookup.get_bounds()
                            )
                        bounds.append(boundary)
                    feeder_proxies.append(
                        FeederProxy.factory(
                            IndexLookup,
                            index,
                            bounds=bounds
                        )
                    )
                    for field in match[:len(valid_field_lookups)]:
                        del fields_map[field]

    # add rest optimizations, i.e. fts lookups
    for token in rest:
        index_lookup = token.get_index_lookup(container_type, order_by)
        if index_lookup is not None:
            feeder_proxies.append(index_lookup)

    if feeder_proxies:
        if len(feeder_proxies) == 1:
            return feeder_proxies[0]
        else:
            feeder_proxies.sort(key=lambda f: f.func.is_ordered_by(order_by))
            return FeederProxy.factory(
                Intersection,
                *feeder_proxies
            )


class DisjunctionToken:
    __slots__ = 'token', 'feeder'

    def __init__(self, token, feeder):
        self.token = token
        self.feeder = feeder


def disjunction_optimizer(tokens,
                          container_type, order_by) -> Optional[partial]:
    dis_tokens = [
        DisjunctionToken(
            token,
            token.get_index_lookup(container_type, order_by)
        )
        for token in tokens
    ]
    dis_tokens.sort(
        key=lambda f: -1 if f.feeder is None
        else f.feeder.func.default_priority
    )
    first_feeder = dis_tokens[0].feeder
    last_feeder = dis_tokens[-1].feeder

    if first_feeder is not None:
        # all optimized
        return FeederProxy.factory(
            Union,
            *[t.feeder for t in dis_tokens]
        )
    elif last_feeder.func.is_of_type(FTSIndexLookup):
        # add fts feeders
        feeder_proxies = []
        while last_feeder is not None \
                and last_feeder.func.is_of_type(FTSIndexLookup):
            feeder_proxies.append(dis_tokens.pop().feeder)
            last_feeder = dis_tokens[-1].feeder
        # build filter_func for remaining tokens
        sources = [token.token.source() for token in dis_tokens]
        filter_func = eval(f'lambda i, s, v: {" or ".join(sources)}',
                           environment)
        feeder_proxies.append(FeederProxy.factory(
            CollectionFeeder,
            filter_func=filter_func
        ))
        return FeederProxy.factory(
            Union,
            *feeder_proxies
        )
