import abc
import asyncio
from collections import defaultdict

from porcupine import exceptions
from porcupine.config import settings


class AbstractTransaction(object, metaclass=abc.ABCMeta):
    __slots__ = ('connector', 'options', '_inserted', '_externals',
                 '_deleted', '_modified', '_sd', '_appends')

    txn_max_retries = settings['db'].get('txn_max_retries', 12)
    UPSERT_MUT = 0

    def __init__(self, connector, **options):
        self.connector = connector
        self.connector.active_txns += 1
        self.options = options
        self._inserted = {}
        self._externals = {}
        self._deleted = {}
        self._modified = {}
        # sub document mutations
        self._sd = defaultdict(dict)
        self._appends = defaultdict(str)

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

    async def prepare(self):
        # call changed attributes event handlers till snapshots are drained
        while True:
            all_items = {**self._inserted, **self._modified}
            snapshots = {item.id: item.__snapshot__
                         for item in all_items.values()
                         if item.__snapshot__}
            # clear snapshots
            for item_id in snapshots:
                all_items[item_id].__snapshot__ = {}
            # print(snapshots)
            if snapshots:
                for item_id, snapshot in snapshots.items():
                    item = all_items[item_id]
                    for attr, old_value in snapshot.items():
                        attr_def = item.__schema__[attr]
                        try:
                            on_change = attr_def.on_change(
                                item,
                                getattr(item, attr_def.storage)[attr],
                                old_value)
                            if asyncio.iscoroutine(on_change):
                                await on_change
                        except exceptions.AttributeSetError as e:
                            raise exceptions.InvalidUsage(str(e))
            else:
                break

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
            append_keys = list(self._appends.keys())
            tasks = [connector.exists(key) for key in append_keys]
            completed, _ = await asyncio.wait(tasks)
            keys_exist = [c.result() for c in completed]
            insertions = {key: '' for key, exists in keys_exist
                          if not exists}
        return insertions, upsertions

    @abc.abstractmethod
    def commit(self):
        """
        Commits the transaction.

        @return: None
        """
        self.connector.active_txns -= 1

    @abc.abstractmethod
    def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        self.connector.active_txns -= 1
