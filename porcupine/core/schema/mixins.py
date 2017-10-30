from typing import Dict, List

from porcupine.hinting import TYPING
from porcupine import db, context, exceptions, gather
from porcupine.core.context import system_override
from porcupine.core.datatypes.system import Deleted, ParentId
from porcupine.core import utils
from porcupine.core.utils import permissions, date
from porcupine.datatypes import Embedded, Composition


class Cloneable(TYPING.ITEM_TYPE):
    """
    Adds cloning capabilities to Porcupine Objects.

    Adding I{Cloneable} to the base classes of a class
    makes instances of this class cloneable, allowing item copying.
    """
    __slots__ = ()

    @staticmethod
    async def _prepare_id_map(item: 'Cloneable',
                              id_map: Dict[str, str],
                              is_root: bool=False) -> List['Cloneable']:
        all_items = []

        id_map[item.id] = utils.generate_oid()

        if not is_root:
            all_items.append(item)

        for attr_name, data_type in item.__schema__.items():
            if isinstance(data_type, Embedded):
                embedded = await getattr(item, attr_name).item()
                if embedded is not None:
                    await Cloneable._prepare_id_map(embedded, id_map)
            elif isinstance(data_type, Composition):
                composites = getattr(item, attr_name).items()
                await gather(*[Cloneable._prepare_id_map(c, id_map)
                               async for c in composites])

        if item.is_collection:
            # runs with system override - exclude deleted
            children = [Cloneable._prepare_id_map(child, id_map)
                        for child in await item.get_children()
                        if not child.is_deleted]
            for items in await gather(*children):
                all_items += items
        return all_items

    @staticmethod
    async def _write_clone(item: 'Cloneable',
                           memo: Dict,
                           target: TYPING.CONTAINER_CO=None) -> 'Cloneable':
        id_map = memo['_id_map_']
        clone = await item.clone(memo)

        if target is not None:
            # clone.inherit_roles = False
            copy_num = 1
            original_name = clone.name
            while await target.child_exists(clone.name):
                copy_num += 1
                clone.name = '{0} ({1})'.format(original_name, copy_num)
            if clone.is_collection:
                await target.containers.add(clone)
            else:
                await target.items.add(clone)

        clone.parent_id = id_map[item.parent_id]
        await context.txn.insert(clone)
        return clone

    async def _copy(self,
                    target: TYPING.CONTAINER_CO,
                    memo: Dict) -> 'Cloneable':
        all_children = await Cloneable._prepare_id_map(
            self, memo['_id_map_'], is_root=True)
        memo['_id_map_'][self.parent_id] = target.id
        clone = await Cloneable._write_clone(self, memo, target)
        for item in all_children:
            await Cloneable._write_clone(item, memo)
        return clone

    async def copy_to(self, target: TYPING.CONTAINER_CO) -> 'Cloneable':
        """
        Copies the item to the designated target.

        @param target: The target container
        @type target: L{Container}
        @return: L{Item}
        """
        if self.is_collection and await target.is_contained_in(self):
            raise exceptions.InvalidUsage(
                'Cannot copy item to destination. '
                'The destination is contained in the source.')

        # check permissions on target folder
        user = context.user
        user_role = await permissions.resolve(target, user)
        if user_role < permissions.AUTHOR:
            raise exceptions.Forbidden('Forbidden')
        with system_override():
            return await self._copy(target,
                                    {'_dup_ext_': True,
                                     '_id_map_': {}})


class Movable(TYPING.ITEM_TYPE):
    """
    Adds moving capabilities to Porcupine Objects.

    Adding I{Movable} to the base classes of a class
    makes instances of this class movable, allowing item moving.
    """
    __slots__ = ()

    parent_id = ParentId()

    async def move_to(self, target: TYPING.CONTAINER_CO) -> None:
        """
        Moves the item to the designated target.

        @param target: The target container
        @type target: L{Container}
        @return: None
        """
        if self.is_system:
            raise exceptions.Forbidden('Forbidden')

        user = context.user
        user_role = await permissions.resolve(self, user)
        can_move = (
            (user_role > permissions.AUTHOR) or
            (user_role == permissions.AUTHOR and self.owner == user.id)
        )
        user_role2 = await permissions.resolve(target, user)

        if self.is_collection and await target.is_contained_in(self):
            raise exceptions.InvalidUsage(
                'Cannot move item to destination. '
                'The destination is contained in the source.')

        if not can_move or user_role2 < permissions.AUTHOR:
            raise exceptions.Forbidden('Forbidden')

        with system_override():
            parent_id = self.parent_id
            self.parent_id = target.id
            self.modified = date.utcnow()
            parent = await db.connector.get(parent_id)

            if self.is_collection:
                await target.containers.add(self)
                await parent.containers.remove(self)
            else:
                await target.items.add(self)
                await parent.items.remove(self)

            await context.txn.upsert(self)


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
        if not context.system_override:
            user = context.user
            user_role = await permissions.resolve(self, user)
            can_delete = (
                (user_role > permissions.AUTHOR) or
                (user_role == permissions.AUTHOR and self.owner == user.id)
            )
        else:
            can_delete = True

        if not can_delete or self.is_system:
            raise exceptions.Forbidden('Forbidden')

        with system_override():
            if self.parent_id is not None:
                parent = await db.connector.get(self.parent_id)
                if parent is not None:
                    # update parent
                    if self.is_collection:
                        await parent.containers.remove(self)
                    else:
                        await parent.items.remove(self)
            await context.txn.delete(self)

    @db.transactional()
    async def delete(self, request):
        await self.remove()
        return True


class Recyclable(TYPING.ITEM_TYPE):
    __slots__ = ()

    is_deleted = Deleted()

    async def restore(self) -> None:
        user = context.user
        user_role = await permissions.resolve(self, user)
        can_restore = (
            (user_role > permissions.AUTHOR) or
            (user_role == permissions.AUTHOR and self.owner == user.id)
        )
        if not can_restore:
            raise exceptions.Forbidden('Forbidden')

        with system_override():
            utils.add_uniques(self)
            self.is_deleted -= 1
            await context.txn.upsert(self)
            await context.txn.restore(self)

    async def recycle_to(self, recycle_bin: TYPING.RECYCLE_BIN_CO) -> None:
        """
        Moves the item to the specified recycle bin.
        The item then becomes inaccessible.

        @param recycle_bin: The recycle bin container, which must be
                            a L{RecycleBin} instance
        @type recycle_bin: RecycleBin
        @return: None
        """
        user = context.user
        user_role = await permissions.resolve(self, user)
        can_delete = (
            (user_role > permissions.AUTHOR) or
            (user_role == permissions.AUTHOR and self.owner == user.id)
        )
        if not can_delete or self.is_system:
            raise exceptions.Forbidden('Forbidden')

        from .recycle import DeletedItem, RecycleBin
        if not isinstance(recycle_bin, RecycleBin):
            raise TypeError("'{0}' is not instance of RecycleBin"
                            .format(type(recycle_bin).__name__))
        with system_override():
            utils.remove_uniques(self)
            # mark as deleted
            self.is_deleted += 1
            await context.txn.upsert(self)
            await context.txn.recycle(self)
            deleted = DeletedItem(deleted_item=self)
            deleted.location = await self.full_path(include_self=False)
            await deleted.append_to(recycle_bin)
