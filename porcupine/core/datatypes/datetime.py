from porcupine.core.utils import date
from porcupine.core.datatypes.common import String


class DateTime(String):
    """Date data type"""
    allow_none = True

    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        iso_string = super().__get__(instance, owner)
        if iso_string is not None:
            return date.get(iso_string)

    def __set__(self, instance, value):
        if isinstance(value, str):
            # we need to validate
            value = date.get(value)
        super().__set__(instance, value.isoformat())


class Date(DateTime):
    """Datetime data type"""
    def __get__(self, instance, owner):
        if instance is None:
            return self
        arrow_obj = super().__get__(instance, owner)
        arrow_obj.date_only = True
        return arrow_obj

    def __set__(self, instance, value):
        if isinstance(value, str):
            # we need to validate
            value = date.get(value)
        String.__set__(self, instance, value.date().isoformat())
