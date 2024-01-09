from typing import AsyncIterable


class Cursor(AsyncIterable):
    def __init__(self, connector, statement):
        self.connector = connector
        self.statement = statement

    async def __aiter__(self):
        results = await self.connector.db.execute(str(self.statement))
        for row in results:
            yield row
