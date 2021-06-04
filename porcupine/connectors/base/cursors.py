import abc
from typing import AsyncIterable

from porcupine.connectors.base.bounds import RangedBoundary
from porcupine.core.stream.streamer import IdStreamer


class BaseCursor(IdStreamer, metaclass=abc.ABCMeta):
    def __init__(self, index, **options):
        self.index = index
        self.options = options
        super().__init__(self.get_iterator())

    @abc.abstractmethod
    def get_iterator(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class BaseIterator(AsyncIterable, metaclass=abc.ABCMeta):
    def __init__(self, index):
        self.index = index

    @abc.abstractmethod
    def __aiter__(self):
        raise NotImplementedError


###############################
# Secondary Index Base Cursor #
###############################

class SecondaryIndexCursor(BaseCursor, metaclass=abc.ABCMeta):
    supports_reversed_iteration = True

    @property
    def is_ranged(self):
        return self._iterator.is_ranged

    @property
    def bounds(self):
        return self._iterator.bounds

    def set_scope(self, scope):
        self._iterator.set_scope(scope)

    def set(self, v: list):
        self._iterator.set(v)

    def reverse(self):
        self._iterator.reverse()
        return super().reverse()

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(scope={repr(self._iterator.scope)}, '
            f'index={repr(self.index.name)}, '
            f'bounds={repr(self._iterator.bounds)}, '
            f'reversed={repr(self._reversed)})'
        )


class SecondaryIndexIterator(BaseIterator, metaclass=abc.ABCMeta):
    def __init__(self, index):
        super().__init__(index)
        self._bounds = [RangedBoundary()]
        self._reversed = False
        self._scope = None

    @property
    def is_ranged(self):
        return not self._bounds[-1].is_fixed

    @property
    def bounds(self):
        return self._bounds

    @property
    def scope(self):
        return self._scope

    def set_scope(self, scope):
        self._scope = scope

    def set(self, v: list):
        self._bounds = v

    def reverse(self):
        self._reversed = True


#########################
# FTS Index Base Cursor #
#########################

class FTSIndexCursor(BaseCursor, metaclass=abc.ABCMeta):
    supports_reversed_iteration = True

    @property
    def scope(self):
        return self._iterator.scope

    def set_scope(self, scope):
        self._iterator.set_scope(scope)

    def set_term(self, term):
        self._iterator.set_term(term)

    def set_type(self, typ):
        self._iterator.set_type(typ)

    def reverse(self):
        self._iterator.reverse()
        return super().reverse()

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(scope={repr(self._iterator.scope)}, '
            f'term={repr(self._iterator.term)}, '
            f'type={repr(self._iterator.type)}, '
            f'reversed={repr(self._reversed)})'
        )


class FTSIndexIterator(BaseIterator, metaclass=abc.ABCMeta):
    def __init__(self, index):
        super().__init__(index)
        self._term = None
        self._scope = None
        self._type = 'query'
        self._reversed = False

    @property
    def term(self):
        return self._term

    @property
    def scope(self):
        return self._scope

    @property
    def type(self):
        return self._type

    def set_scope(self, scope):
        self._scope = scope

    def set_term(self, term):
        self._term = term

    def set_type(self, typ):
        self._type = typ

    def reverse(self):
        self._reversed = True
