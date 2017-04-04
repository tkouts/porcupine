from namedlist import namedlist


class Storage(namedlist('Storage', '')):
    __slots__ = ()

    @property
    def fields(self):
        return self._fields

    def as_dict(self):
        return self._asdict()


def storage(typename, field_names, rename=False):
    nl = namedlist(typename, field_names, default=None, rename=rename)
    return type(typename, (nl, Storage, ), {})
