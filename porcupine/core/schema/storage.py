from namedlist import namedlist


class Storage(namedlist('Storage', '')):
    __slots__ = ()

    @classmethod
    def fields(cls):
        return getattr(cls, '_fields')

    def update(self, *args, **kwargs):
        return self._update(*args, **kwargs)

    def as_dict(self):
        return self._asdict()


def storage(typename, field_names, rename=False) -> namedlist:
    nl = namedlist(typename, field_names, default=None, rename=rename)
    return type(typename, (nl, Storage, ), {})
