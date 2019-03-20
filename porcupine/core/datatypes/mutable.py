from .datatype import MutableDataType


class List(MutableDataType):
    """List data type"""
    safe_type = list

    def __init__(self, default=None, **kwargs):
        if default is None and not kwargs.get('allow_none'):
            default = []
        super().__init__(default, **kwargs)


class Dictionary(MutableDataType):
    """Dictionary data type"""
    safe_type = dict

    def __init__(self, default=None, **kwargs):
        if default is None and not kwargs.get('allow_none'):
            default = {}
        super().__init__(default, **kwargs)
