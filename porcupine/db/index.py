from typing import Sequence


class Index:
    def __init__(self, on, unique=False, where=None):
        if not isinstance(on, Sequence):
            on = on,
        self.on = on
        self.unique = unique
        self.where = where

    def __hash__(self):
        return hash((self.on, self.unique, self.where))

    def __eq__(self, other: 'Index'):
        return (
            self.on == other.on
            and self.unique == other.unique
            and self.where == other.where
        )


class FTSIndex:
    def __init__(self, on, locale='english'):
        if not isinstance(on, Sequence):
            on = on,
        self.on = on
        self.locale = locale

    def __hash__(self):
        return hash((self.on, self.locale))

    def __eq__(self, other: 'FTSIndex'):
        return (
            self.on == other.on
            and self.locale == other.locale
        )
