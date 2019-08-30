from porcupine.core.utils import date
from porcupine.datatypes import DataType


class DateTime(DataType):
    """Datetime data type"""
    safe_type = date.DateTime

    def __init__(self, default=None, **kwargs):
        super().__init__(default, allow_none=True, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None and isinstance(value, str):
            # convert to pendulum date
            value = date.get(value, date_only=self.safe_type == date.Date)
            storage = getattr(instance, self.storage)
            setattr(storage, self.storage_key, value)
        return value

    def __set__(self, instance, value):
        if value is not None:
            date_only = self.safe_type == date.Date
            if not isinstance(value, self.safe_type):
                # we need to validate
                value = date.get(value, date_only=date_only)
            if not date_only:
                # convert to utc
                value = value.in_timezone('UTC')
        super().__set__(instance, value)


class Date(DateTime):
    """Date data type"""
    safe_type = date.Date
