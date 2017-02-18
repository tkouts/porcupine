import copy

from porcupine.datatypes import String
from porcupine.utils import system
from .elastic import Elastic


class Composite(Elastic):
    """
    Objects within Objects...

    Think of this as an embedded item. This class is useful
    for implementing compositions. Instances of this class
    are embedded into other items.
    Note that instances of this class have no
    security descriptor since they are embedded into other items.
    The L{security} property of such instances is actually a proxy to
    the security attribute of the object that embeds this object.
    Moreover, they do not have parent containers the way
    instances of L{GenericItem} have.

    @see: L{porcupine.datatypes.Composition}
    """
    # __image__ = "desktop/images/object.gif"
    name = String(required=True)

    @property
    def security(self) -> dict:
        """The object's security descriptor.
        A proxy to the descriptor of the enclosing object.

        @rtype: dict
        """
        return db._db.get_item(self._pid[1:]).security

    def clone(self, memo=None):
        """
        Creates an in-memory clone of the item.
        This is a shallow copy operation meaning that the item's
        references are not cloned.

        @return: the clone object
        @rtype: L{GenericItem}
        """
        if memo is None:
            memo = {
                '_dup_ext_': True,
                '_id_map_': {}
            }
        new_id = memo['_id_map_'].get(self._id, system.generate_oid())
        memo['_id_map_'][self._id] = new_id
        clone = copy.deepcopy(self)
        # call data types clone method
        [dt.clone(clone, memo) for dt in self.__schema__.values()]
        clone._id = new_id
        return clone
