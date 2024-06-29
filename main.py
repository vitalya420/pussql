import asyncio
from collections import namedtuple

from executors.sqlite import SQLiteExecutor
from manager import DatabaseManager
from query import get_default, Query
from pussql.factory import Factory

balls_queries = Query(folder="balls")
query = get_default()


async def test(cursor):
    print('in test')
    res = await cursor.fetchall()
    print(res)
    columns = [description[0] for description in cursor.description]
    print(columns)


user_factory = Factory("User", class_=namedtuple)


class Prikol(SQLiteExecutor):

    @query('SELECT * FROM test;', factory=user_factory(many=True))
    async def select_users(self): ...

    @query('SELECT * FROM test LIMIT 1;', factory=user_factory(class_=dict, many=True))
    async def select_user(self): ...


async def main():
    manager = DatabaseManager('examples/database.db')
    async with manager.session() as session:
        async with session.begin():
            prikols = Prikol(session)
            print(await prikols.select_users())
            print(await prikols.select_users())
            print(await prikols.select_users())
            print(await prikols.select_users())
            print(await prikols.select_users())
            print(await prikols.select_users())



asyncio.run(main())
