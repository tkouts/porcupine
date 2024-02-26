# from asyncio import gather
# from typing import Dict, List

from porcupine.hinting import TYPING
from porcupine import exceptions
from porcupine.core.context import system_override, context
from porcupine.core.services import db_connector
from porcupine.core.datatypes.system import Deleted
from porcupine.core.utils import date
# from porcupine.datatypes import Embedded, Composition


class Cloneable(TYPING.ITEM_TYPE):
    """
    Adds cloning capabilities to Porcupine Objects.

    Adding I{Cloneable} to the base classes of a class
    makes instances of this class cloneable, allowing item copying.
    """
    __slots__ = ()

    async def copy_to(self, target: TYPING.CONTAINER_CO) -> 'Cloneable':
        """
        Copies the item to the designated target.

        @param target: The target container
        @type target: L{Container}
        @return: L{Item}
        """
        # if self.is_collection and await target.is_contained_in(self):
        #     raise exceptions.InvalidUsage(
        #         'Cannot copy item to destination. '
        #         'The destination is contained in the source.')

        # check permissions on target folder
        if not await target.can_update(context.user):
            raise exceptions.Forbidden('Forbidden')

        clone = await self.clone({'_dup_ext_': True})

        if target is not None:
            copy_num = 1
            original_name = clone.name
            while await target.child_exists(clone.name):
                copy_num += 1
                clone.name = f'{original_name} ({copy_num})'
            await target.children.add(clone)

        return clone


class Movable(TYPING.ITEM_TYPE):
    """
    Adds moving capabilities to Porcupine Objects.

    Adding I{Movable} to the base classes of a class
    makes instances of this class movable, allowing item moving.
    """
    __slots__ = ()

    async def move_to(self, target: TYPING.CONTAINER_CO) -> None:
        """
        Moves the item to the designated target.

        @param target: The target container
        @type target: L{Container}
        @return: None
        """
        if self.is_collection and await target.is_contained_in(self):
            raise exceptions.InvalidUsage(
                'Cannot move item to destination. '
                'The destination is contained in the source.'
            )

        parent = await context.db.get(self.parent_id)

        with system_override():
            self.modified = date.utcnow()
            self.modified_by = context.user.name
            self.p_type = target.content_class

        await super(type(parent.children), parent.children).remove(self)
        await super(type(target.children), target.children).add(self)

            # self.parent_id = target.id
        # await self.touch()
        # await context.db.txn.upsert(self)


class Removable(TYPING.ITEM_TYPE):
    """
    Makes Porcupine objects removable.

    Adding I{Removable} to the base classes of a class
    makes instances of this type removable.
    Instances of this type can be either logically
    deleted (moved to a L{RecycleBin} instance) or physically
    deleted.
    """
    __slots__ = ()

    async def remove(self) -> None:
        """
        Deletes the item permanently.

        @return: None
        """
        if self.parent_id is not None:
            parent = await context.db.get(self.parent_id)
            if parent is not None:
                await parent.children.remove(self)
        else:
            # root item
            can_delete = await self.can_delete(context.user)
            if not can_delete:
                raise exceptions.Forbidden('Forbidden')
            await context.db.txn.delete(self)


class Recyclable(TYPING.ITEM_TYPE):
    __slots__ = ()

    is_deleted = Deleted()

    async def restore(self) -> None:
        can_restore = await self.can_delete(context.user)
        if not can_restore:
            raise exceptions.Forbidden('Forbidden')

        with system_override():
            self.is_deleted -= 1
        await context.db.txn.upsert(self)
        await context.db.txn.restore(self)

    async def recycle_to(self, recycle_bin: TYPING.RECYCLE_BIN_CO) -> None:
        """
        Moves the item to the specified recycle bin.
        The item then becomes inaccessible.

        @param recycle_bin: The recycle bin container, which must be
                            a L{RecycleBin} instance
        @type recycle_bin: RecycleBin
        @return: None
        """
        can_delete = await self.can_delete(context.user)
        if not can_delete:
            raise exceptions.Forbidden('Forbidden')

        from .recycle import DeletedItem, RecycleBin
        if not isinstance(recycle_bin, RecycleBin):
            raise TypeError("'{0}' is not instance of RecycleBin"
                            .format(type(recycle_bin).__name__))

        with system_override():
            deleted = DeletedItem(deleted_item=self)
            deleted.location = await self.full_path(include_self=False)
            await deleted.append_to(recycle_bin)
            # mark as deleted
            self.is_deleted += 1
        await context.db.txn.upsert(self)
        await context.db.txn.recycle(self)
