from typing import AsyncIterable


class Cursor(AsyncIterable):
    def __init__(self, connector, statement, **params):
        self.connector = connector
        self.statement = statement
        self.params = params

    async def __aiter__(self):
        results = await self.connector.db.execute(
            self.statement.get_sql(),
            self.params
        )
        for row in results:
            yield row
