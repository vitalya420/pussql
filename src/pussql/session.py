from contextlib import asynccontextmanager
from typing import Optional, Iterable, Type, TypeVar

import aiosqlite

from pussql.exception import Rollback

T = TypeVar('T')


class Session:
    def __init__(self, db_path, manager):
        self.db_path = db_path
        self.connection = None
        self.manager = manager
        self.executor = None

    async def close(self):
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def connect(self):
        if self.connection is None:
            self.connection = await aiosqlite.connect(self.db_path)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @asynccontextmanager
    async def begin(self):
        async with self:
            try:
                await self.execute("BEGIN")
                yield self
                await self.execute("COMMIT")
            except Rollback:
                await self.execute("ROLLBACK")
                return
            except:
                await self.execute("ROLLBACK")
                raise

    async def execute(self, query: str, params: Optional[Iterable] = None):
        cursor = await self.connection.execute(query, params)
        return cursor

    def __call__(self, executor: Type[T]) -> T:
        return executor(self)
