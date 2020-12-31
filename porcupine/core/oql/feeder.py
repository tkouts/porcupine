from typing import Iterable
from functools import partial

from namedlist import namedlist

from porcupine.core.services import db_connector
from porcupine.core.utils.date import DateTime, Date
from porcupine.connectors.base.cursor import Range
from porcupine.core.stream.streamer import EmptyStreamer
from porcupine.pipe import filter, chain

__all__ = (
    'DynamicRange',
    'CollectionFeeder',
    'IndexLookup',
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
    def __init__(self, *args, **kwargs):
        self.ordered_by = None
        self.desc = False

    def __call__(self, statement, scope, v):
        raise NotImplementedError


class CollectionFeeder(namedlist('CollectionFeeder',
                                 'item collection reversed',
                                 default=None), Feeder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.desc = False

    @property
    def ordered_by(self):
        if self.collection == 'children':
            return 'is_collection'
        return None

    def __call__(self, statement, scope, v):
        item = self.item
        # TODO: check we have a valid collection
        if self.collection == 'children':
            feeder = item.containers | chain(item.items)
        else:
            feeder = getattr(item, self.collection)
        if self.reversed:
            feeder.reverse()
        return feeder


class IndexLookup(
        namedlist(
            'IndexLookup',
            'index_type index_name bounds reversed filter_func',
            default=None
        ), Feeder):
    def __init__(self, *args, **kwargs):
        self.options = kwargs.pop('options', {})
        super().__init__(*args, **kwargs)
        Feeder.__init__(self)
        self.ordered_by = self.index_name

    @staticmethod
    def date_to_utc(date: Date):
        if isinstance(date, DateTime):
            date = date.in_timezone('UTC')
        return date.isoformat()

    def __call__(self, statement, scope, v):
        feeder = db_connector().indexes[self.index_type][self.index_name].\
            get_cursor(**self.options)
        feeder.set_scope(scope)
        if self.bounds is not None:
            bounds = self.bounds(None, statement, v)
            if isinstance(bounds, Range):
                if isinstance(bounds.l_bound, Date):
                    bounds.l_bound = self.date_to_utc(bounds.l_bound)
                if isinstance(bounds.u_bound, Date):
                    bounds.u_bound = self.date_to_utc(bounds.u_bound)
            elif isinstance(bounds, Date):
                bounds = self.date_to_utc(bounds)
            feeder.set(bounds)
        if self.reversed:
            feeder.reverse()
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder


class Intersection(namedlist('Intersection', 'first second'), Feeder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Feeder.__init__(self)

    def __call__(self, statement, scope, v):
        first_feeder = self.first(statement, scope, v)
        second_feeder = self.second(statement, scope, v)
        if self.first.index_name == self.second.index_name:
            ranged = next(feeder for feeder in (first_feeder, second_feeder)
                          if feeder.is_ranged)
            if ranged:
                other = (second_feeder if ranged is first_feeder
                         else first_feeder)
                inter = ranged.bounds.intersection(other.bounds)
                # print('INTER', inter)
                if inter:
                    first_feeder.set(inter)
                    return first_feeder
                return EmptyStreamer()
        return first_feeder.intersection(second_feeder)


class Union(namedlist('Union', 'first second'), Feeder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Feeder.__init__(self)

    def __call__(self, statement, scope, v):
        first_feeder = self.first(statement, scope, v)
        second_feeder = self.second(statement, scope, v)
        if self.first.index_name == self.second.index_name:
            ranged = next(feeder for feeder in (first_feeder, second_feeder)
                          if feeder.is_ranged)
            if ranged:
                other = (second_feeder if ranged is first_feeder
                         else first_feeder)
                union = ranged.bounds.union(other.bounds)
                # print('UNION', union)
                if union:
                    first_feeder.set(union)
                    return first_feeder
        return first_feeder.union(second_feeder)
