from arrow import api, Arrow


class JsonArrow(Arrow):
    date_only = False

    def __json__(self):
        if self.date_only:
            return f'"{self.date().isoformat()}"'
        return f'"{self.isoformat()}"'


_factory = api.factory(JsonArrow)


def get(*args, **kwargs) -> JsonArrow:
    return _factory.get(*args, **kwargs)


def utcnow() -> JsonArrow:
    return _factory.utcnow()


def now(tz=None) -> JsonArrow:
    return _factory.now(tz)
