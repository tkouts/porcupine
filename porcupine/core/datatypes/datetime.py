from datetime import datetime
from porcupine.core.utils import date
from porcupine.datatypes import String


class DateTime(String):
    """Datetime data type"""

    def __init__(self, default=None, **kwargs):
        super().__init__(default, allow_none=True, **kwargs)

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
        super().__set__(instance, value and value.isoformat())


class Date(DateTime):
    """Date data type"""

    def __get__(self, instance, owner):
        if instance is None:
            return self
        arrow_obj = super().__get__(instance, owner)
        if arrow_obj is not None:
            arrow_obj.date_only = True
        return arrow_obj

    def __set__(self, instance, value):
        if isinstance(value, str):
            # we need to validate
            value = date.get(value)
        if isinstance(value, datetime):
            value = value.date()
        String.__set__(self, instance, value and value.isoformat())
