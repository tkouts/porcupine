from namedlist import namedlist


class Storage(namedlist('Storage', '')):
    __slots__ = ()

    @classmethod
    def fields(cls) -> tuple:
        return cls._fields

    def update(self, *args, **kwargs) -> None:
        return self._update(*args, **kwargs)

    def as_dict(self) -> dict:
        # do not persist None as it is the default value - see below
        return {k: v for k, v in zip(self.fields(), self)
                if v is not None}


def storage(typename, field_names, rename=False) -> namedlist:
    nl = namedlist(typename, field_names, default=None, rename=rename)
    return type(typename, (nl, Storage, ), {})
