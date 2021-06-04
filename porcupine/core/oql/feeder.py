import itertools
from functools import partial

from porcupine.core.stream.streamer import EmptyStreamer
from porcupine.pipe import filter, chain
from porcupine.connectors.base.bounds import EMPTY

__all__ = (
    'Feeder',
    'CollectionFeeder',
    'EmptyFeeder',
    'IndexLookup',
    'FTSIndexLookup',
    'Intersection',
    'Union',
)


class Feeder:
    __slots__ = 'reversed', 'options', 'filter_func'
    optimized = True
    default_priority = 0

    def __init__(self, options=None, filter_func=None):
        self.reversed = False
        self.options = options or {}
        self.filter_func = filter_func

    @staticmethod
    def is_ordered_by(field_list):
        return field_list is None

    @classmethod
    def is_of_type(cls, feeder_type):
        return cls is feeder_type

    def get_streamer(self, statement, item, collection, v):
        raise NotImplementedError

    def __call__(self, statement, item, collection, v):
        streamer = self.get_streamer(statement, item, collection, v)
        if self.filter_func is not None:
            flt = partial(self.filter_func, s=statement, v=v)
            streamer = streamer.items() | filter(flt)
        return streamer


class EmptyFeeder(Feeder):
    default_priority = 2

    def get_streamer(self, statement, item, collection, v):
        return EmptyStreamer()


class CollectionFeeder(Feeder):
    optimized = False

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

    def get_streamer(self, statement, item, collection, v):
        # TODO: check we have a valid collection
        if collection == 'children':
            feeder = item.containers | chain(item.items)
        else:
            feeder = getattr(item, collection)
        if not self.reversed:
            feeder.reverse()
        return feeder


class IndexLookup(Feeder):
    __slots__ = 'index', 'bounds'
    default_priority = 1

    def __init__(self, index, bounds=None, options=None, filter_func=None):
        super().__init__(options, filter_func)
        self.bounds = bounds
        self.index = index

    def is_ordered_by(self, field_list):
        if field_list is not None:
            return field_list in itertools.combinations(self.index.attr_list,
                                                        len(field_list))
        return True

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'index_type={repr(self.index.container_type.__name__)}, '
            f'index_name={repr(self.index.name)}, '
            f'bounds={repr(self.bounds)}, '
            f'reversed={repr(self.reversed)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def get_streamer(self, statement, item, collection, v):
        streamer = self.index.get_cursor(**self.options)
        streamer.set_scope(item.id)
        if self.bounds is not None:
            bounds = []
            for b in self.bounds:
                boundary = b(statement, v)
                if boundary is EMPTY:
                    return EmptyStreamer()
                bounds.append(boundary)
            streamer.set(bounds)
        if self.reversed:
            streamer.reverse()
        return streamer


class FTSIndexLookup(Feeder):
    __slots__ = 'index', 'term', 'field', 'query_type'
    default_priority = 2

    def __init__(self, index, term, field, query_type,
                 options=None, filter_func=None):
        super().__init__(options, filter_func)
        self.index = index
        self.term = term
        self.field = field
        self.query_type = query_type

    @staticmethod
    def is_ordered_by(field_list):
        return field_list == ('_score', )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'index_type={repr(self.index.container_type.__name__)}, '
            f'term={repr(self.term)}, '
            f'field={repr(self.field)}, '
            f'type={repr(self.query_type)}, '
            f'reversed={repr(self.reversed)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def get_streamer(self, statement, item, collection, v):
        feeder = self.index.get_cursor(**self.options)
        feeder.set_scope(item.id)
        feeder.set_term(self.term(statement, v))
        feeder.set_type(self.query_type)
        if self.reversed:
            feeder.reverse()
        return feeder


class Intersection(Feeder):
    __slots__ = 'feeders'

    def __init__(self, *feeders, options=None, filter_func=None):
        super().__init__(options, filter_func)
        self.feeders = feeders

    def is_ordered_by(self, field_list):
        return self.feeders[-1].is_ordered_by(field_list)

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'feeders={repr(self.feeders)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def get_streamer(self, statement, item, collection, v):
        streamers = [f(statement, item, collection, v) for f in self.feeders]
        if self.reversed:
            streamers[-1].reverse()
        streamer = streamers[0]
        for f in streamers[1:]:
            streamer = streamer.intersection(f)
        return streamer


class Union(Feeder):
    __slots__ = 'feeders'

    def __init__(self, *feeders, options=None, filter_func=None):
        super().__init__(options, filter_func)
        self.feeders = feeders

    def __repr__(self):
        return (
            f'{self.__class__.__name__}('
            f'feeders={repr(self.feeders)}, '
            f'filter_func={repr(self.filter_func)}'
            ')'
        )

    def get_streamer(self, statement, item, collection, v):
        streamers = [f(statement, item, collection, v) for f in self.feeders]
        if self.reversed:
            streamers[-1].reverse()
        streamer = streamers[0]
        for f in streamers[1:]:
            streamer = streamer.union(f)
        return streamer
