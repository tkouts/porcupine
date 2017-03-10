# import logging
import asyncio
from collections import defaultdict
from porcupine import exceptions
import couchbase
# from couchbase.exceptions import KeyExistsError
import couchbase.subdocument as sub_doc
from porcupine.core.db.transaction import AbstractTransaction


class Transaction(AbstractTransaction):
    UPSERT_MUT = 0

    def __init__(self, connector, **options):
        super().__init__(connector, **options)
        self._inserted = {}
        self._externals = {}
        self._deleted = {}
        self._modified = {}
        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = defaultdict(str)
        self.done = False

    def __contains__(self, key):
        return key in self._inserted or key in self._modified \
               or key in self._deleted or key in self._externals

    def __getitem__(self, key):
        if key in self._deleted:
            return None
        elif key in self._modified:
            return self._modified[key]
        elif key in self._inserted:
            return self._inserted[key]
        elif key in self._externals:
            return self._externals[key]
        raise KeyError

    def insert(self, item):
        if item.id in self._inserted:
            raise exceptions.DBAlreadyExists
        self._inserted[item.id] = item

    def update(self, item):
        self._modified[item.id] = item

    def mutate(self, item, path, mutation_type, value):
        self._sd[item.id][path] = (mutation_type, value)

    def append(self, key, value):
        self._appends[key] += value

    def put_external(self, key, value):
        self._externals[key] = value

    # def delete(self, item, object_id=None):
    #     if object_id is None:
    #         object_id = item['_id']
    #     if object_id not in self._locks:
    #         # we need to lock
    #         r = self.get(object_id)
    #         if r.cas:
    #             self._locks[object_id].is_removed = True
    #             # set stale doc
    #             if '_owner' in item:
    #                 self.mark_parent_as_stale(item)
    #                 # delete stale value
    #                 if item['is_collection']:
    #                     self.remove_stale_doc(item)
    #         else:
    #             # it is new or already deleted
    #             del self._locks[object_id]
    #     else:
    #         self._locks[object_id].is_removed = True
    #         if self._locks[object_id].cas == 0:
    #             del self._locks[object_id]
    #         elif '_owner' in item:
    #             self.mark_parent_as_stale(item)
    #             # delete stale value
    #             if item['is_collection']:
    #                 self.remove_stale_doc(item)

    async def prepare(self):
        # call changed attributes event handlers
        for item in {**self._inserted, **self._modified}.values():
            for attr, old_value in item.__snapshot__.items():
                attr_def = item.__schema__[attr]
                on_change = attr_def.on_change(
                    item, getattr(item, attr_def.storage)[attr], old_value)
                if asyncio.iscoroutine(on_change):
                    await on_change

        connector = self.connector
        dumps = connector.persist.dumps

        # upsertions
        upsertions = {k: dumps(i) for k, i in self._inserted.items()}
        # merge externals
        upsertions.update(self._externals)

        # insertions
        insertions = {}
        if self._appends:
            # make sure externals with appends are initialized
            append_keys = self._appends.keys()
            tasks = [connector.exists(key) for key in append_keys]
            completed, _ = await asyncio.wait(tasks)
            keys_exist = [c.result for c in completed]
            insertions = {k: '' for k, v in zip(append_keys, keys_exist)
                          if not v}
        return insertions, upsertions

    async def commit(self):
        """
        Commits the transaction.

        @return: None
        """
        insertions, upsertions = await self.prepare()
        bucket = self.connector.bucket

        if not self.done:
            tasks = []
            # insertions
            if insertions:
                tasks.append(bucket.insert_multi(insertions,
                                                 format=couchbase.FMT_UTF8))
            # upsertions
            if upsertions:
                tasks.append(bucket.upsert_multi(upsertions,
                                                 format=couchbase.FMT_AUTO))
            # sub document mutations
            for item_id, paths in self._sd.items():
                mutations = []
                for path, mutation in paths.items():
                    mutation_type, value = mutation
                    if mutation_type == self.UPSERT_MUT:
                        mutations.append(sub_doc.upsert(path, value))
                tasks.append(bucket.mutate_in(item_id, *mutations))
            # appends
            if self._appends:
                tasks.append(bucket.append_multi(self._appends))

            if tasks:
                completed, _ = await asyncio.wait(tasks)
                errors = [task.exception() for task in tasks]
                if any(errors):
                    # TODO: log errors
                    pass

            self.done = True
            super().commit()

    async def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        if not self.done:
            self.done = True
            super().abort()
