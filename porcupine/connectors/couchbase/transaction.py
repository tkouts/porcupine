# import logging
import asyncio
from inspect import isawaitable

from porcupine.core.abstract.connector.transaction import AbstractTransaction


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
        connector = self.connector

        if not self.done:
            tasks = []
            # insertions
            if insertions:
                task = connector.insert_multi(insertions)
                if isawaitable(task):
                    tasks.append(task)

            # upsertions
            if upsertions:
                task = connector.upsert_multi(upsertions)
                if isawaitable(task):
                    tasks.append(task)

            # sub document mutations
            for item_id, mutations in self._sd.items():
                task = connector.mutate_in(item_id, mutations)
                if isawaitable(task):
                    tasks.append(task)

            # appends
            if self._appends:
                task = connector.append_multi(self._appends)
                if isawaitable(task):
                    tasks.append(task)

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
