from contextlib import asynccontextmanager

from pussql.session import Session


class Pussql:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def session(self):
        """
        New Session object
        :return:
        """
        session = Session(self.db_path, self)
        async with session:
            yield session

    @asynccontextmanager
    async def transaction(self):
        """
        New Session with transaction
        :return:
        """
        session = Session(self.db_path, self)
        async with session.begin():
            yield session
