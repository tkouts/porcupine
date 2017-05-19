from typing import TYPE_CHECKING, TypeVar, Dict, List, Any, ClassVar, Type

DT_CO = Any
ITEM_CO = Any
ANY_ITEM_CO = Any
COMPOSITE_CO = Any
CONTAINER_CO = Any
RECYCLE_BIN_CO = Any
ITEM_ID = Any
ID_LIST = Any
SCHEMA = Any
STORAGE = Any
ITEM_TYPE = object

if TYPE_CHECKING:
    from porcupine.core.schema.item import Item
    from porcupine.core.schema.composite import Composite
    from porcupine.core.schema.container import Container
    from porcupine.core.schema.recycle import RecycleBin
    from porcupine.core.schema.storage import storage
    from porcupine.core.datatypes.datatype import DataType

    # Data types
    DT_CO = TypeVar('DT', DataType, covariant=True)

    # Items
    ITEM_CO = TypeVar('ITEM', Item, covariant=True)
    ANY_ITEM_CO = TypeVar('ANY_ITEM_CO', Item, Composite, covariant=True)
    COMPOSITE_CO = TypeVar('COMPOSITE_CO', Composite, covariant=True)
    CONTAINER_CO = TypeVar('CONTAINER_CO', Container, covariant=True)
    RECYCLE_BIN_CO = TypeVar('RECYCLE_BIN_CO', RecycleBin, covariant=True)
    ITEM_TYPE = Type[ITEM_CO]

    # Other
    ITEM_ID = TypeVar('ITEM_ID', str)
    ID_LIST = TypeVar('ID_LIST', List[ITEM_ID])
    SCHEMA = ClassVar[Dict[str, DT_CO]]
    STORAGE = ClassVar[storage]
