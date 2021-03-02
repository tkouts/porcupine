import itertools
from typing import Iterable
from functools import partial

from namedlist import namedlist

from porcupine.core.services import db_connector
from porcupine.core.utils.date import DateTime, Date
from porcupine.connectors.base.cursors import Range
from porcupine.core.stream.streamer import EmptyStreamer
from porcupine.pipe import filter, chain

__all__ = (
    'Feeder',
    'DynamicRange',
    'CollectionFeeder',
    'EmptyFeeder',
    'IndexLookup',
    'FTSIndexLookup',
    'Intersection',
    'Union',
)


class DynamicRange(namedlist('DynamicRange',
                             'l_bound l_inclusive u_bound, u_inclusive',
                             default=None)):
    def __call__(self: Iterable, _, statement, v):
        bounds = [x(None, statement, v) if callable(x) else x for x in self]
        return Range(*bounds)


class Feeder:
    __slots__ = 'reversed', 'options'
    optimized = True
    default_priority = 0

    def __init__(self, options):
        self.reversed = False
        self.options = options

    @staticmethod
    def is_ordered_by(field_list):
        return field_list is None

    def __call__(self, statement, item, collection, v):
        raise NotImplementedError


class EmptyFeeder(Feeder):
    default_priority = 2

    def __call__(self, statement, item, collection, v):
        return EmptyStreamer()


class CollectionFeeder(Feeder):
    __slots__ = 'filter_func'
    optimized = False

    def __init__(self, filter_func=None, options=None):
        super().__init__(options)
        self.filter_func = filter_func

    @staticmethod
    def is_ordered_by(field_list):
        return field_list == ('is_collection', )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'reversed={repr(self.reversed)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def __call__(self, statement, item, collection, v):
        # TODO: check we have a valid collection
        if collection == 'children':
            feeder = item.items | chain(item.containers)
        else:
            feeder = getattr(item, collection)
        if self.reversed:
            feeder.reverse()
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class IndexLookup(Feeder):
    __slots__ = 'index_type', 'index_name', 'bounds', 'filter_func', 'index'
    default_priority = 1

    def __init__(self, index_type, index_name, bounds=None, filter_func=None,
                 options=None):
        super().__init__(options)
        self.index_type = index_type
        self.index_name = index_name
        self.bounds = bounds
        self.filter_func = filter_func
        self.index = db_connector().views[index_type][index_name]

    def is_ordered_by(self, field_list):
        if field_list is not None:
            return field_list in itertools.combinations(self.index.attr_list,
                                                        len(field_list))
        return True

    @staticmethod
    def date_to_utc(date: Date):
        if isinstance(date, DateTime):
            date = date.in_timezone('UTC')
        return date.isoformat()

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'index_type={repr(self.index_type.__name__)}, '
            f'index_name={repr(self.index_name)}, '
            f'bounds={repr(self.bounds)}, '
            f'reversed={repr(self.reversed)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def __call__(self, statement, item, collection, v):
        streamer = self.index.get_cursor(**self.options)
        streamer.set_scope(item.id)
        if self.bounds is not None:
            bounds = []
            for b in self.bounds:
                boundary = b(None, statement, v)
                if isinstance(boundary, Range):
                    if isinstance(boundary.l_bound, Date):
                        boundary.l_bound = self.date_to_utc(boundary.l_bound)
                    if isinstance(boundary.u_bound, Date):
                        boundary.u_bound = self.date_to_utc(boundary.u_bound)
                elif isinstance(boundary, Date):
                    boundary = self.date_to_utc(boundary)
                bounds.append(boundary)
            streamer.set(bounds)
        if self.reversed:
            streamer.reverse()
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            streamer = streamer.items() | filter(flt)
        return streamer


class FTSIndexLookup(Feeder):
    __slots__ = 'index_type', 'field', 'term', 'filter_func', 'index'
    default_priority = 2

    def __init__(self, index_type, field, term, filter_func=None, options=None):
        super().__init__(options)
        self.index_type = index_type
        self.field = field
        self.term = term
        self.filter_func = filter_func
        self.index = db_connector().fts_indexes[index_type]

    @staticmethod
    def is_ordered_by(field_list):
        return field_list == ('_score', )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'index_type={repr(self.index_type.__name__)}, '
            f'field={repr(self.field)}, '
            f'term={repr(self.term)}, '
            f'reversed={repr(self.reversed)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def __call__(self, statement, item, collection, v):
        feeder = self.index.get_cursor(**self.options)
        feeder.set_scope(item.id)
        feeder.set_term(self.term(statement, v))
        if self.reversed:
            feeder.reverse()
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class Intersection(Feeder):
    __slots__ = 'first', 'second'

    def __init__(self, first, second, options=None):
        super().__init__(options)
        self.first = first
        self.second = second

    def is_ordered_by(self, field_list):
        return self.second.is_ordered_by(field_list)

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'first={repr(self.first)}, '
            f'second={repr(self.second)}'
            ')'
        )

    @property
    def feeders(self):
        return self.first, self.second

    def __call__(self, statement, item, collection, v):
        first_feeder = self.first(statement, item, collection, v)
        second_feeder = self.second(statement, item, collection, v)
        is_same_index = (
            all([isinstance(f, IndexLookup) for f in self.feeders])
            and self.first.index_name == self.second.index_name
            and len(first_feeder.bounds) == len(second_feeder.bounds)
        )
        if is_same_index:
            ranged = next(feeder for feeder in self.feeders
                          if feeder.is_ranged)
            if ranged:
                other = (second_feeder if ranged is first_feeder
                         else first_feeder)
                inter = ranged.bounds[-1].intersection(other.bounds[-1])
                # print('INTER', inter)
                if inter:
                    ranged.bounds[-1] = inter
                    return ranged
                return EmptyStreamer()
        if self.reversed:
            second_feeder.reverse()
        return first_feeder.intersection(second_feeder)


class Union(Feeder):
    __slots__ = 'first', 'second'

    def __init__(self, first, second, options=None):
        super().__init__(options or {})
        self.first = first
        self.second = second

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'first={repr(self.first)}, '
            f'second={repr(self.second)}'
            ')'
        )

    @property
    def feeders(self):
        return self.first, self.second

    def __call__(self, statement, item, collection, v):
        first_feeder = self.first(statement, item, collection, v)
        second_feeder = self.second(statement, item, collection, v)
        is_same_index = (
            all([isinstance(f, IndexLookup) for f in self.feeders])
            and self.first.index_name == self.second.index_name
            and len(first_feeder.bounds) == len(second_feeder.bounds)
        )
        if is_same_index:
            ranged = next(feeder for feeder in self.feeders
                          if feeder.is_ranged)
            if ranged:
                other = (second_feeder if ranged is first_feeder
                         else first_feeder)
                union = ranged.bounds[-1].union(other.bounds[-1])
                # print('UNION', union)
                if union:
                    ranged.bounds[-1] = union
                    return ranged
        return first_feeder.union(second_feeder)
