from typing import AsyncIterable
from porcupine.core.schema.partial import PartialItem


class Cursor(AsyncIterable):
    def __init__(self, connector, query, params):
        self.connector = connector
        self.query = query
        self.params = params

    async def __aiter__(self):
        results = await self.connector.db.execute(
            self.query.get_sql(),
            self.params
        )
        for row in results:
            yield row
