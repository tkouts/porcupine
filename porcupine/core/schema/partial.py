from typing import Mapping
from porcupine.core.utils import get_content_class


class PartialItem:
    __slots__ = '_partial', '_content_class'

    def __init__(self, partial=Mapping):
        self._partial = partial
        self._content_class = get_content_class(partial['type'])

    @property
    def __is_new__(self):
        return False

    @property
    def is_composite(self):
        return self._content_class.is_composite

    @property
    def is_collection(self):
        return self._content_class.is_collection

    @property
    def content_class(self):
        return self._partial['type']

    def __getattr__(self, item):
        try:
            return self._partial[item]
        except KeyError:
            raise AttributeError(
                f"Partial[{self.content_class}]"
                f" object has no attribute '{item}'"
            )

    # @property
    # def parent_id(self):
    #     try:
    #         return self._partial['parent_id']
    #     except KeyError:
    #         raise AttributeError(
    #             f"'{self.content_class}' object has no attribute 'parent_id'"
    #         )
    #
    # @property
    # def item_id(self):
    #     try:
    #         return self._partial['item_id']
    #     except KeyError:
    #         raise AttributeError(
    #             f"'{self.content_class}' object has no attribute 'item_id'"
    #         )
