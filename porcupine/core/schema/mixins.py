import time
import copy

from porcupine import exceptions
from porcupine.datatypes import Embedded, Composition
from porcupine.utils import system
from .elastic import Elastic


class Cloneable(object):
    """
    Adds cloning capabilities to Porcupine Objects.

    Adding I{Cloneable} to the base classes of a class
    makes instances of this class cloneable, allowing item copying.
    """

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

    def clone(self: Elastic, memo: dict = None):
        """
        Creates an in-memory clone of the item.
        This is a shallow copy operation meaning that the item's
        references are not cloned.

        @param memo: internal helper object
        @type memo: dict
        @return: the clone object
        @rtype: L{GenericItem}
        """
        if memo is None:
            memo = {
                '_dup_ext_': True,
                '_id_map_': {}
            }
        new_id = memo['_id_map_'].get(self.id, system.generate_oid())
        memo['_id_map_'][self.id] = new_id
        clone = copy.deepcopy(self)
        # call data types clone method
        [dt.clone(clone, memo) for dt in self.__schema__.values()]
        clone._id = new_id
        now = time.time()
        user = context.user
        clone._owner = user.id
        clone._created = now
        clone._pid = None
        clone._pids = []
        clone.modified_by = user.name
        clone.modified = now
        return clone

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


class Movable(object):
    """
    Adds moving capabilities to Porcupine Objects.

    Adding I{Movable} to the base classes of a class
    makes instances of this class movable, allowing item moving.
    """

    def _update_pids(self, path_info):
        db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing
        if self.children_count:
            path_depth, destination_pids = path_info
            cursor = db._db.get_children(self.id, deep=db_supports_deep_indexing)
            cursor.enforce_permissions = False
            for child in cursor:
                child._pids = destination_pids + child._pids[path_depth:]
                db._db.put_item(child)
                if not db_supports_deep_indexing and child.is_collection:
                    self._update_pids(path_info)
            cursor.close()

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


class Removable(object):
    """
    Makes Porcupine objects removable.

    Adding I{Removable} to the base classes of a class
    makes instances of this type removable.
    Instances of this type can be either logically
    deleted - (moved to a L{RecycleBin} instance) - or physically
    deleted.
    """

    def _delete(self, _update_parent=True):
        """
        Deletes the item physically.

        @return: None
        """
        db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing
        if self.is_collection and self.children_count and \
                (_update_parent or not db_supports_deep_indexing):
            cursor = db._db.get_children(self._id, deep=db_supports_deep_indexing)
            cursor.enforce_permissions = False
            [child._delete(False) for child in cursor]
            cursor.close()

        db._db.handle_delete(self, True)
        db._db.delete_item(self)

        if _update_parent and not self._is_deleted:
            # update container modification timestamp
            parent = db._db.get_item(self._pid, get_lock=False)
            if parent is not None:
                # update parent
                if self.is_collection:
                    parent._nc.decr(1)
                else:
                    parent._ni.decr(1)
                parent.modified = time.time()

        db._db.handle_post_delete(self, True)

    # @db.requires_transactional_context
    def delete(self):
        """
        Deletes the item permanently.

        @return: None
        """
        user = context.user
        self_ = db._db.get_item(self.id)

        user_role = permsresolver.get_access(self_, user)
        can_delete = (user_role > permsresolver.AUTHOR) or \
                     (user_role == permsresolver.AUTHOR and self_._owner == user._id)

        if not self_._is_system and can_delete:
            # delete item physically
            self_._delete()
        else:
            raise exceptions.Forbidden(
                'The object was not deleted.\n'
                'The user has insufficient permissions.')

    def _recycle(self, _update_parent=True):
        """
        Deletes an item logically.
        Bypasses security checks.

        @return: None
        """
        is_deleted = self._is_deleted

        if not is_deleted:
            db._db.handle_delete(self, False)
            # db._db.delete_item(self)

        self._is_deleted = int(self._is_deleted) + 1
        db._db.put_item(self)

        if _update_parent:
            # update parent
            parent = db._db.get_item(self._pid, get_lock=False)
            if self.is_collection:
                parent._nc.decr(1)
            else:
                parent._ni.decr(1)
            parent.modified = time.time()

        if not is_deleted:
            db._db.handle_post_delete(self, False)

        db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing
        if self.is_collection and self.children_count and \
                (_update_parent or not db_supports_deep_indexing):
            cursor = db._db.get_children(self._id, deep=db_supports_deep_indexing)
            cursor.enforce_permissions = False
            [child._recycle(False) for child in cursor]
            cursor.close()

    def _undelete(self, path_info=None):
        """
        Undeletes a logically deleted item.
        Bypasses security checks.

        @return: None
        """
        self._is_deleted = int(self._is_deleted) - 1
        if not self._is_deleted:
            db._db.handle_undelete(self)

        if path_info is None:
            # update parent
            parent = db._db.get_item(self._pid, get_lock=False)
            if self.is_collection:
                parent._nc.incr(1)
            else:
                parent._ni.incr(1)
            parent.modified = time.time()

            # calculate path depth and destination _pids
            path_depth = len(self._pids)
            self._pids = parent._pids + [parent._id]
            destination_pids = self._pids
        else:
            path_depth, destination_pids = path_info
            # update parent ids
            self._pids = destination_pids + self._pids[path_depth:]

        db_supports_deep_indexing = db._db._db_handle.supports_deep_indexing
        if self.is_collection and self.children_count and \
                (not path_info or not db_supports_deep_indexing):
            cursor = db._db.get_children(self._id, deep=db_supports_deep_indexing)
            cursor.enforce_permissions = False
            [child._undelete((path_depth, destination_pids)) for child in cursor]
            cursor.close()

        db._db.put_item(self)

    # @db.requires_transactional_context
    def recycle(self, rb_id):
        """
        Moves the item to the specified recycle bin.
        The item then becomes inaccessible.

        @param rb_id: The id of the destination container, which must be
                      a L{RecycleBin} instance
        @type rb_id: str
        @return: None
        """
        user = context.user
        self_ = db._db.get_item(self._id)

        user_role = permsresolver.get_access(self_, user)
        can_delete = (user_role > permsresolver.AUTHOR) or \
                     (user_role == permsresolver.AUTHOR and
                      self_._owner == user._id)

        if not self_._is_system and can_delete:
            deleted = DeletedItem(self_)
            deleted._owner = user._id
            deleted._created = time.time()
            deleted.modified_by = user.name
            deleted.modified = time.time()
            deleted._pid = rb_id

            # check recycle bin's containment
            recycle_bin = db._db.get_item(rb_id, get_lock=False)
            if deleted.contentclass not in recycle_bin.containment:
                raise exceptions.ContainmentError(
                    'The target container does not accept '
                    'objects of type\n"%s".'  % deleted.contentclass)

            db._db.handle_update(deleted, None)
            db._db.put_item(deleted)
            db._db.handle_post_update(deleted, None)

            recycle_bin._ni.incr(1)

            # delete item logically
            self_._recycle()
        else:
            raise exceptions.Forbidden(
                'The object was not deleted.\n'
                'The user has insufficient permissions.')
