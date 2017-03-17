# import logging
import asyncio
import couchbase
import couchbase.subdocument as sub_doc

from porcupine.core.abstract.db.transaction import AbstractTransaction


class Transaction(AbstractTransaction):
    __slots__ = ('done', )

    def __init__(self, connector, **options):
        super().__init__(connector, **options)
        self.done = False

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
