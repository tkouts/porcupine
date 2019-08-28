from typing import Iterable
from functools import partial

from namedlist import namedlist

from porcupine.core.services import db_connector
from porcupine.core.utils.date import JsonArrow
from porcupine.connectors.base.cursor import Range
from porcupine.pipe import filter

__all__ = (
    'DynamicRange',
    'IndexLookup',
)


class DynamicRange(namedlist('DynamicRange',
                             'l_bound l_inclusive u_bound, u_inclusive',
                             default=None)):
    def __call__(self: Iterable, _, statement, v):
        bounds = [x(None, statement, v) if callable(x) else x for x in self]
        return Range(*bounds)


class Feeder:
    @property
    def sort_order(self):
        return None

    def __call__(self, statement, scope, v):
        raise NotImplementedError


class IndexLookup(namedlist('IndexLookup',
                            'index_name bounds reversed filter_func',
                            default=None), Feeder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def sort_order(self):
        return self.index_name

    @staticmethod
    def date_to_utc(date):
        return date.to('UTC').for_json()

    def __call__(self, statement, scope, v):
        feeder = db_connector().indexes[self.index_name].get_cursor()
        feeder.set_scope(scope)
        if self.bounds is not None:
            bounds = self.bounds(None, statement, v)
            if isinstance(bounds, Range):
                if isinstance(bounds.l_bound, JsonArrow):
                    bounds.l_bound = self.date_to_utc(bounds.l_bound)
                if isinstance(bounds.u_bound, JsonArrow):
                    bounds.u_bound = self.date_to_utc(bounds.u_bound)
            elif isinstance(bounds, JsonArrow):
                bounds = self.date_to_utc(bounds)
            feeder.set(bounds)
        if self.reversed:
            feeder.reverse()
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            feeder = feeder.items() | filter(flt)
        return feeder
