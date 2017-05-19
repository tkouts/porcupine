from typing import TYPE_CHECKING, TypeVar, Dict, List, Any, ClassVar, Type


class _Hinter:
    def __init__(self):
        if TYPE_CHECKING:
            ##############
            # Data types #
            ##############

            from porcupine.datatypes import DataType, Composition, Embedded

            self.DT_CO = TypeVar('DT', DataType, covariant=True)
            self.DT_COMPOSITION_CO = TypeVar('DT_COMPOSITION_CO',
                                             Composition, Embedded,
                                             covariant=True)
            self.DT_COMPOSITION_TYPE = Type[self.DT_COMPOSITION_CO]

            ####################
            # Data type values #
            ####################

            from porcupine.core.datatypes.composition import EmbeddedItem, \
                EmbeddedCollection

            self.COMPOSITION_CO = TypeVar('COMPOSITION_CO',
                                          EmbeddedItem, EmbeddedCollection,
                                          covariant=True)
            self.COMPOSITION_TYPE = Type[self.COMPOSITION_CO]

            #########
            # Items #
            #########

            from porcupine.schema import Item, Composite, Container
            from porcupine.core.schema.recycle import RecycleBin

            self.ITEM_CO = TypeVar('ITEM_CO', Item, covariant=True)
            self.ANY_ITEM_CO = TypeVar('ANY_ITEM_CO', Item, Composite,
                                       covariant=True)
            self.COMPOSITE_CO = TypeVar('COMPOSITE_CO', Composite,
                                        covariant=True)
            self.CONTAINER_CO = TypeVar('CONTAINER_CO', Container,
                                        covariant=True)
            self.RECYCLE_BIN_CO = TypeVar('RECYCLE_BIN_CO', RecycleBin,
                                          covariant=True)
            self.ITEM_TYPE = Type[self.ITEM_CO]

            #########
            # Other #
            #########

            from porcupine.core.schema.storage import storage

            self.ITEM_ID = TypeVar('ITEM_ID', str)
            self.ID_LIST = TypeVar('ID_LIST', List[self.ITEM_ID])
            self.SCHEMA = ClassVar[Dict[str, self.DT_CO]]
            self.STORAGE = ClassVar[storage]

    def __getattr__(self, item):
        if item.endswith('_TYPE'):
            return object
        return Any

TYPING = _Hinter()
