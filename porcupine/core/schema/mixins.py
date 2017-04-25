import datetime

from porcupine import db, context, exceptions, gather, view
from porcupine.contract import contract
from porcupine.core.context import system_override
from porcupine.datatypes import Embedded, Composition
from porcupine.core.datatypes.system import Deleted
from porcupine.utils import system, permissions
from .elastic import Elastic


class Cloneable:
    """
    Adds cloning capabilities to Porcupine Objects.

    Adding I{Cloneable} to the base classes of a class
    makes instances of this class cloneable, allowing item copying.
    """
    __slots__ = ()

    @staticmethod
    def _prepare_id_map(item, id_map, is_root=False):
        children = []
        db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing

        if '.' in item.id:
            path = item.id.split('.')[:-1]
            new_path = [id_map.get(oid, oid) for oid in path]
            new_path.append(system.generate_oid())
            id_map[item.id] = '.'.join(new_path)
        else:
            id_map[item.id] = system.generate_oid()

        if not is_root:
            children.append(item)

        for attr_name, attr_def in item.__schema__.items():
            if isinstance(attr_def, Embedded):
                attr = getattr(item, attr_name)
                if attr:
                    Cloneable._prepare_id_map(attr, id_map)
            elif isinstance(attr_def, Composition):
                attr = getattr(item, attr_name)
                [Cloneable._prepare_id_map(i, id_map) for i in attr]

        if isinstance(item, GenericItem) \
                and item.is_collection \
                and item.children_count \
                and (is_root or not db_supports_deep_indexing):
            cursor = db._db.get_children(item.id,
                                         deep=db_supports_deep_indexing)
            for child in cursor:
                children += Cloneable._prepare_id_map(child, id_map)
            cursor.close()
        return children

    @staticmethod
    def _write_clone(item, memo, target):
        id_map = memo['_id_map_']
        clone = item.clone(memo)

        if target is not None:
            clone.inherit_roles = False
            copy_num = 1
            original_name = clone.name
            while target.child_exists(clone.name):
                copy_num += 1
                clone.name = '{} ({})'.format(original_name, copy_num)

        clone._pid = id_map.get(item.parent_id, item.parent_id)
        clone._pids = memo['_dest_pids_'] + \
            [id_map[pid] for pid in item._pids[memo['_source_depth_']:]]

        db._db.handle_update(clone, None, check_unique=False)
        db._db.put_item(clone)
        db._db.handle_post_update(clone, None)

        if clone.is_collection:
            if clone.children_count:
                reversed_ids = {v: k for k, v in id_map.items()}
                # clear any items created by event handlers
                [child.delete() for child in clone.get_children()
                 if child.id not in reversed_ids]
            # maintain the same values for _nc and _ni as the original
            clone._ni = item._ni
            clone._nc = item._nc

    def _copy(self: Elastic, target: Elastic, memo: dict) -> None:
        all_children = Cloneable._prepare_id_map(self, memo['_id_map_'], True)
        memo['_id_map_'][self.parent_id] = target.id
        memo['_dest_pids_'] = target._pids + [target.id]
        memo['_source_depth_'] = len(self._pids)
        Cloneable._write_clone(self, memo, target)
        for item in all_children:
            Cloneable._write_clone(item, memo, None)
            # item._write_clone(memo, None)

    # def _write_clone(self, memo, target):
    #     id_map = memo['_id_map_']
    #     clone = self.clone(memo)
    #
    #     if target is not None:
    #         clone.inherit_roles = False
    #         copy_num = 1
    #         original_name = clone.name
    #         while target.child_exists(clone.name):
    #             copy_num += 1
    #             clone.name = '%s (%d)' % (original_name, copy_num)
    #
    #     clone._pid = id_map.get(self._pid, self._pid)
    #     clone._pids = memo['_dest_pids_'] + [id_map[pid]
    #           for pid in self._pids[memo['_source_depth_']:]]
    #
    #     db._db.handle_update(clone, None, check_unique=False)
    #     db._db.put_item(clone)
    #     db._db.handle_post_update(clone, None)
    #
    #     if clone.is_collection:
    #         if clone.children_count:
    #             reversed_ids = {v: k for k, v in id_map.items()}
    #             # clear any items created by event handlers
    #             [child.delete() for child in clone.get_children()
    #              if child._id not in reversed_ids]
    #         # maintain the same values for _nc and _ni as the original
    #         clone._ni = self._ni
    #         clone._nc = self._nc

    # def clone(self: Elastic, memo: dict = None):
    #     """
    #     Creates an in-memory clone of the item.
    #     This is a shallow copy operation meaning that the item's
    #     references are not cloned.
    #
    #     @param memo: internal helper object
    #     @type memo: dict
    #     @return: the clone object
    #     @rtype: L{GenericItem}
    #     """
    #     if memo is None:
    #         memo = {
    #             '_dup_ext_': True,
    #             '_id_map_': {}
    #         }
    #     new_id = memo['_id_map_'].get(self.id, system.generate_oid())
    #     memo['_id_map_'][self.id] = new_id
    #     clone = copy.deepcopy(self)
    #     # call data types clone method
    #     [dt.clone(clone, memo) for dt in self.__schema__.values()]
    #     clone._id = new_id
    #     now = time.time()
    #     user = context.user
    #     clone._owner = user.id
    #     clone._created = now
    #     clone._pid = None
    #     clone._pids = []
    #     clone.modified_by = user.name
    #     clone.modified = now
    #     return clone

    # @db.requires_transactional_context
    def copy_to(self: Elastic, target):
        """
        Copies the item to the designated target.

        @param target: The id of the target container or the container object
                       itself
        @type target: str OR L{Container}
        @return: None
        @raise L{porcupine.exceptions.ObjectNotFound}:
            If the target container does not exist.
        """
        if isinstance(target, str):
            target = db._db.get_item(target, get_lock=False)

        if target is None or target._is_deleted:
            raise exceptions.ObjectNotFound(
                'The target container does not exist.')

        content_class = self.contentclass

        if self.is_collection and target.is_contained_in(self.id):
            raise exceptions.ContainmentError(
                'Cannot copy item to destination.\n'
                'The destination is contained in the source.')

        # check permissions on target folder
        user = context.user
        user_role = permsresolver.get_access(target, user)
        if not self._is_system and user_role > permsresolver.READER:
            if content_class not in target.containment:
                raise exceptions.ContainmentError(
                    'The target container does not accept '
                    'objects of type\n"%s".' % content_class)
            self._copy(target, {'_dup_ext_': True, '_id_map_': {}})
            # update parent
            if self.is_collection:
                target._nc.incr(1)
            else:
                target._ni.incr(1)
            target.modified = time.time()
        else:
            raise exceptions.Forbidden(
                'The object was not copied.\n'
                'The user has insufficient permissions.')


class Movable:
    """
    Adds moving capabilities to Porcupine Objects.

    Adding I{Movable} to the base classes of a class
    makes instances of this class movable, allowing item moving.
    """
    __slots__ = ()

    # def _update_pids(self, path_info):
    #     db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing
    #     if self.children_count:
    #         path_depth, destination_pids = path_info
    #         cursor = db._db.get_children(self.id,
    #  deep=db_supports_deep_indexing)
    #         cursor.enforce_permissions = False
    #         for child in cursor:
    #             child._pids = destination_pids + child._pids[path_depth:]
    #             db._db.put_item(child)
    #             if not db_supports_deep_indexing and child.is_collection:
    #                 self._update_pids(path_info)
    #         cursor.close()

    # @db.requires_transactional_context
    def move_to(self, target, inherit_roles=False):
        """
        Moves the item to the designated target.

        @param target: The id of the target container or the container object
                       itself
        @type target: str OR L{Container}
        @return: None
        @raise L{porcupine.exceptions.ObjectNotFound}:
            If the target container does not exist.
        """
        user = context.user
        user_role = permsresolver.get_access(self, user)
        can_move = user_role > permsresolver.AUTHOR \
            or (user_role == permsresolver.AUTHOR and self.owner == user.id)

        parent_id = self._pid
        if isinstance(target, (str, bytes)):
            target = db._db.get_item(target, get_lock=False)

        if target is None or target._is_deleted:
            raise exceptions.ObjectNotFound(
                'The target container does not exist.')

        content_class = self.contentclass

        user_role2 = permsresolver.get_access(target, user)

        if self.is_collection and target.is_contained_in(self.id):
            raise exceptions.ContainmentError(
                'Cannot move item to destination.\n'
                'The destination is contained in the source.')

        if not self._is_system and can_move and user_role2 > permsresolver.READER:
            if content_class not in target.containment:
                raise exceptions.ContainmentError(
                    'The target container does not accept '
                    'objects of type\n"%s".' % content_class)

            # db._db.delete_item(self)
            old_item = db._db.get_item(self._id)
            self._pid = target.id
            self._pids = target._pids + [target.id]

            # calculate path depth and destination _pids
            path_info = (len(old_item._pids), self._pids)

            if inherit_roles:
                self.inherit_roles = True
                if target.security != self.security:
                    self._apply_security(target, False)
            else:
                self.inherit_roles = False
            self.modified = time.time()

            db._db.handle_update(self, old_item)
            db._db.put_item(self)

            parent = db._db.get_item(parent_id, get_lock=False)
            # update target and parent
            if self.is_collection:
                target._nc.incr(1)
                parent._nc.decr(1)
            else:
                target._ni.incr(1)
                parent._ni.decr(1)
            target.modified = time.time()
            parent.modified = time.time()
            db._db.handle_post_update(self, old_item)

            if self.is_collection:
                self._update_pids(path_info)
        else:
            raise exceptions.Forbidden(
                'The object was not moved.\n'
                'The user has insufficient permissions.')


class Removable:
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
        async def _delete(item):
            if item.is_system:
                raise exceptions.Forbidden(
                    'The object {0} is systemic and can not be removed.'
                    .format(item.name))
            if item.is_collection:
                children = await item.get_children()
                await gather(*[_delete(child) for child in children])
            context.txn.delete(item)

        user = context.user
        user_role = await permissions.resolve(self, user)
        can_delete = (
            (user_role > permissions.AUTHOR) or
            (user_role == permissions.AUTHOR and self.owner == user.id)
        )

        if can_delete:
            with system_override():
                if self.parent_id is not None:
                    parent = await db.connector.get(self.parent_id)
                    if parent is not None:
                        # update parent
                        if self.is_collection:
                            parent.containers.remove(self)
                        else:
                            parent.items.remove(self)
                        parent.modified = datetime.datetime.utcnow().isoformat()
                        context.txn.upsert(parent)
                await _delete(self)
        else:
            raise exceptions.Forbidden(
                'The object was not deleted.\n'
                'The user has insufficient permissions.')

    @db.transactional()
    async def delete(self, request):
        await self.remove()
        return True


class Recyclable:
    __slots__ = ()

    async def recycle_to(self, recycle_bin):
        """
        Moves the item to the specified recycle bin.
        The item then becomes inaccessible.

        @param recycle_bin: The recycle bin container, which must be
                            a L{RecycleBin} instance
        @type recycle_bin: RecycleBin
        @return: None
        """
        async def _recycle(item):
            if item.is_system:
                raise exceptions.Forbidden(
                    'The object {0} is systemic and can not be recycled.'
                    .format(item.name))
            if item.is_collection:
                children = await item.get_children()
                await gather(*[_recycle(child) for child in children])

        user = context.user
        user_role = await permissions.resolve(self, user)
        can_delete = (
            (user_role > permissions.AUTHOR) or
            (user_role == permissions.AUTHOR and self.owner == user.id)
        )

        if can_delete:
            from .recycle import DeletedItem
            with system_override():
                await _recycle(self)
                deleted = DeletedItem(deleted_item=self)
                deleted.location = await self.full_path(include_self=False)
                await deleted.append_to(recycle_bin)
                # mark as deleted
                self.deleted = True
                context.txn.upsert(self)
        else:
            raise exceptions.Forbidden(
                'The object was not deleted.\n'
                'The user has insufficient permissions.')

    @contract(accepts=bool)
    @db.transactional()
    async def recycled(self, request):
        if request.json:
            with system_override():
                recycle_bin = await db.get_item('RB')
            await self.recycle_to(recycle_bin)
            return True
        return False
    recycled = view(put=recycled)
