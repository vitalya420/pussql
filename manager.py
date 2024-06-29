from contextlib import asynccontextmanager
from typing import Optional, List

from src.pussql.base.executor import BaseExecutor
# from base import Executor
from src.pussql.session import Session


class DatabaseManager:
    def __init__(self,
                 db_path: Optional[str] = None,
                 executors: Optional[List[BaseExecutor]] = None):
        self.db_path = db_path
        self.executors = set(executors or [])

    @asynccontextmanager
    async def session(self):
        session = Session(self.db_path, self)
        async with session:
            yield session
