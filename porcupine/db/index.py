class Index:
    def __init__(self, on, unique=False, when_value_is=None):
        if isinstance(on, str):
            on = on,
        self.on = on
        self.unique = unique
        if when_value_is and not isinstance(when_value_is, tuple):
            when_value_is = when_value_is,
        self.when_value_is = when_value_is

    def __hash__(self):
        return hash((self.on, self.unique, self.when_value_is))

    def __eq__(self, other: 'Index'):
        return (
            self.on == other.on
            and self.unique == other.unique
            and self.when_value_is == other.when_value_is
        )


class FTSIndex:
    def __init__(self, on, locale='english'):
        if isinstance(on, str):
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
