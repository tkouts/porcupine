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
    priority = 0
    optimized = True

    def __init__(self, options):
        self.ordered_by = None
        self.reversed = False
        self.options = options

    def __call__(self, statement, item, collection, v):
        raise NotImplementedError


class EmptyFeeder(Feeder):
    priority = 2

    def __call__(self, statement, item, collection, v):
        return EmptyStreamer()


class CollectionFeeder(Feeder):
    optimized = False

    def __init__(self, filter_func=None):
        # Feeder.__init__(self, kwargs.pop('options', {}))
        super().__init__(None)
        self.ordered_by = 'is_collection'
        self.filter_func = filter_func

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'reversed={repr(self.reversed)} '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def __call__(self, statement, item, collection, v):
        # TODO: check we have a valid collection
        if collection == 'children':
            feeder = item.containers | chain(item.items)
        else:
            feeder = getattr(item, collection)
        if not self.reversed:
            feeder.reversed = True
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class IndexLookup(
        namedlist(
            'IndexLookup',
            'index_type index_name bounds filter_func',
            default=None
        ), Feeder):
    priority = 1

    def __init__(self, *args, **kwargs):
        Feeder.__init__(self, kwargs.pop('options', {}))
        super().__init__(*args, **kwargs)
        self.ordered_by = self.index_name

    @staticmethod
    def date_to_utc(date: Date):
        if isinstance(date, DateTime):
            date = date.in_timezone('UTC')
        return date.isoformat()

    def __call__(self, statement, item, collection, v):
        type_views = db_connector().views[self.index_type]
        feeder = type_views[self.index_name].get_cursor(**self.options)
        feeder.set_scope(item.id)
        if self.bounds is not None:
            bounds = self.bounds(None, statement, v)
            if isinstance(bounds, Range):
                if isinstance(bounds.l_bound, Date):
                    bounds.l_bound = self.date_to_utc(bounds.l_bound)
                if isinstance(bounds.u_bound, Date):
                    bounds.u_bound = self.date_to_utc(bounds.u_bound)
            elif isinstance(bounds, Date):
                bounds = self.date_to_utc(bounds)
            feeder.set([bounds])
        if self.reversed:
            feeder.reversed = True
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class FTSIndexLookup(
        namedlist(
            'FTSIndexLookup',
            'index_type field term filter_func',
            default=None
        ), Feeder):
    priority = 2

    def __init__(self, *args, **kwargs):
        Feeder.__init__(self, kwargs.pop('options', {}))
        super().__init__(*args, **kwargs)
        self.ordered_by = '_score'
        # self.reversed = True

    def __call__(self, statement, item, collection, v):
        fts_index = db_connector().fts_indexes[self.index_type]
        feeder = fts_index.get_cursor(**self.options)
        feeder.set_scope(item.id)
        feeder.set_term(self.term(statement, v))
        if not self.reversed:
            # default is descending
            feeder.reversed = True
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class Intersection(namedlist('Intersection', 'first second'), Feeder):
    def __init__(self, *args, **kwargs):
        Feeder.__init__(self, kwargs.pop('options', None))
        super().__init__(*args, **kwargs)

    def __call__(self, statement, item, collection, v):
        first_feeder = self.first(statement, item, collection, v)
        second_feeder = self.second(statement, item, collection, v)
        is_same_index = (
            all([isinstance(f, IndexLookup) for f in (self.first, self.second)])
            and self.first.index_name == self.second.index_name
            and len(first_feeder.bounds) == len(second_feeder.bounds)
        )
        if is_same_index:
            ranged = next(feeder for feeder in (first_feeder, second_feeder)
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
        return first_feeder.intersection(second_feeder)


class Union(namedlist('Union', 'first second'), Feeder):
    def __init__(self, *args, **kwargs):
        Feeder.__init__(self, kwargs.pop('options', None))
        super().__init__(*args, **kwargs)
        self.ordered_by = self.second.ordered_by

    def __call__(self, statement, item, collection, v):
        first_feeder = self.first(statement, item, collection, v)
        second_feeder = self.second(statement, item, collection, v)
        is_same_index = (
            all([isinstance(f, IndexLookup) for f in (self.first, self.second)])
            and self.first.index_name == self.second.index_name
            and len(first_feeder.bounds) == len(second_feeder.bounds)
        )
        if is_same_index:
            ranged = next(feeder for feeder in (first_feeder, second_feeder)
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
