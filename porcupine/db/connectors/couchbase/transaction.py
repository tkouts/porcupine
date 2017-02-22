import logging
from couchbase.items import Item, ItemOptionDict
from couchbase.exceptions import TemporaryFailError, NotFoundError
from porcupine.core.db.transaction import AbstractTransaction


class Result(object):
    def __init__(self, value=None, cas=0):
        self.value = value
        self.cas = cas
        self.is_modified = False
        self.is_removed = False


class Transaction(AbstractTransaction):
    lock_ttl = 20

    def __init__(self, connector, **options):
        super().__init__(connector, **options)
        self._locks = {}
        self._atomic = {}
        self._incr = {}
        self.done = False

    def _retry(self):
        self.done = False
        super()._retry()

    def _get_update(self, oid):
        update = self._locks[oid]
        if update.is_removed:
            return Result()
        else:
            return update

    def get(self, object_id):
        if object_id in self._locks:
            return self._get_update(object_id)
        try:
            rv = self.connector.bucket.lock(object_id, ttl=self.lock_ttl)
        except NotFoundError:
            # new item
            return Result()
        except TemporaryFailError:
            logging.warning('Failed to get lock for object %s' % object_id)
            raise exceptions.DBRetryTransaction

        self._locks[object_id] = Result(value=rv.value, cas=rv.cas)
        return rv

    def get_multi(self, object_ids):
        multi = {}
        for oid in object_ids:
            if oid in self._locks:
                multi[oid] = self._get_update(oid)
        # multi lock
        for_lock = [oid for oid in object_ids if oid not in multi]
        if for_lock:
            try:
                multi_result = self.connector.bucket.lock_multi(
                    for_lock, ttl=self.lock_ttl)
            except TemporaryFailError:
                logging.warning('Failed to get lock for objects: {}'.format(
                    ', '.join(object_ids)))
                raise exceptions.DBRetryTransaction
            for oid in multi_result:
                multi[oid] = multi_result[oid]
                self._locks[oid] = Result(
                    value=multi_result[oid].value,
                    cas=multi_result[oid].cas
                )
        return multi

    def mark_parent_as_stale(self, item):
        if item['_pid'] is not None:
            atomic_key = '{}_stale'.format(item['_pid'])
            result = self._atomic.setdefault(atomic_key, Result(value=1))
            result.is_modified = True

    def remove_stale_doc(self, item):
        atomic_key = '{}_stale'.format(item['_id'])
        result = self._atomic.setdefault(atomic_key, Result())
        result.is_removed = True

    def set(self, item, object_id=None):
        is_new = False
        if object_id is None:
            object_id = item['_id'] or self.connector.root_id
        if object_id in self._locks:
            # it is a locked item
            self._locks[object_id].value = item
        else:
            # it is a new item
            # we need to make sure it is locked
            r = self.get(object_id)
            if r.cas:
                self._locks[object_id].value = item
            else:
                # first time insert
                is_new = True
                self._locks[object_id] = Result(value=item)
        self._locks[object_id].is_modified = True
        self._locks[object_id].is_removed = False
        if '_owner' in item:
            self.mark_parent_as_stale(item)
        return is_new

    def delete(self, item, object_id=None):
        if object_id is None:
            object_id = item['_id']
        if object_id not in self._locks:
            # we need to lock
            r = self.get(object_id)
            if r.cas:
                self._locks[object_id].is_removed = True
                # set stale doc
                if '_owner' in item:
                    self.mark_parent_as_stale(item)
                    # delete stale value
                    if item['is_collection']:
                        self.remove_stale_doc(item)
            else:
                # it is new or already deleted
                del self._locks[object_id]
        else:
            self._locks[object_id].is_removed = True
            if self._locks[object_id].cas == 0:
                del self._locks[object_id]
            elif '_owner' in item:
                self.mark_parent_as_stale(item)
                # delete stale value
                if item['is_collection']:
                    self.remove_stale_doc(item)

    # atomic operations
    def get_atomic(self, atomic_id):
        if atomic_id in self._atomic:
            return self._atomic[atomic_id].value

        value = self.connector.get(atomic_id)
        if value is not None:
            self._atomic[atomic_id] = Result(value=value)

        if atomic_id in self._incr:
            # fallback to default
            value = value or self._incr[atomic_id][1]
            # add delta increments
            value += self._incr[atomic_id][0]

        return value

    def set_atomic(self, atomic_id, value):
        self._atomic[atomic_id] = Result(value=value)
        self._atomic[atomic_id].is_modified = True
        self._atomic[atomic_id].is_removed = False

    def delete_atomic(self, atomic_id):
        if atomic_id not in self._atomic:
            self._atomic[atomic_id] = Result()
        self._atomic[atomic_id].is_removed = True

    def increment(self, atomic_id, amount, default):
        if atomic_id in self._incr:
            self._incr[atomic_id][0] += amount
        else:
            self._incr[atomic_id] = [amount, default]

    # scopes
    @staticmethod
    def in_scope(scope, item_dict):
        if scope.startswith('.'):
            return scope[1:] in item_dict.get('_pids', [])
        else:
            return scope == item_dict.get('_pid')

    def get_scope(self, scope_id):
        return {k: v for k, v in self._locks.items()
                if type(v.value) == dict
                and self.in_scope(scope_id, v.value) and not v.cas}

    def commit(self):
        """
        Commits the transaction.

        @return: None
        """

        # import pprint
        # locks = {k: v.value for k, v in self._locks.items()}
        # pprint.pprint(locks)

        if not self.done:
            context.data['stale'] = False
            context.data['__cache'] = {}
            # add new, update existing and remove deleted
            update_items = ItemOptionDict()
            removed_items = ItemOptionDict()
            unlocks = {}
            # print 'locks:', len(self._locks)
            for k, v in self._locks.items():
                item = Item(k, v.value)
                item.cas = v.cas
                if v.is_removed:
                    if v.cas != 0:
                        removed_items.add(item)
                else:
                    if v.is_modified:
                        update_items.add(item)
                    else:
                        unlocks[k] = v.cas
            if unlocks:
                # print 'UNLOCKING: ', unlocks
                try:
                    self.connector.bucket.unlock_multi(unlocks)
                except TemporaryFailError:
                    pass

            # add atomic updates
            for object_id, item in self._atomic.items():
                if item.is_removed:
                    removed_items.add(Item(object_id))
                elif item.is_modified:
                    update_items.add(Item(object_id, item.value))

            # update docs
            if len(update_items) > 0:
                # print 'UPDATING: %d' % len(update_items)
                self.connector.bucket.set_multi(update_items)
            # execute deltas
            for atomic_id, amount in self._incr.items():
                # print atomic_id, amount
                self.connector.bucket.incr(atomic_id, amount[0],
                                           initial=sum(amount))
            # remove deleted
            if len(removed_items) > 0:
                self.connector.bucket.delete_multi(removed_items, quiet=True)

            self.done = True
            super().commit()

    def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        if not self.done:
            if 'stale' in context.data:
                del context.data['stale']
            unlocks = {k: v.cas for k, v in self._locks.items() if v.cas}
            if unlocks:
                try:
                    self.connector.bucket.unlock_multi(unlocks)
                except TemporaryFailError:
                    pass
            self._locks = {}
            self._atomic = {}
            self._incr = {}
            self.done = True
            super().abort()
