import asyncio
from collections import namedtuple
from dataclasses import dataclass
from pprint import pprint
from typing import NamedTuple

from pussql import Pussql, Factory
from pussql import Query
from pussql.exception import Rollback
from pussql.executors import SQLiteExecutor

users_queries = Query(folder='queries/users')
cars_queries = Query(folder='queries/cars')

users_factory = Factory("Users", class_=dataclass)
cars_factory = Factory("Cars", class_=namedtuple)


async def handle_joined_response(cursor):
    data = await cursor.fetchall()
    users = {}

    for row in data:
        user_id = row[0]
        if user_id not in users:
            users[user_id] = {
                "user_id": user_id,
                "user_name": row[1],
                "cars": []
            }

        if row[2] is not None:
            car = {
                "car_id": row[2],
                "car_name": row[3]
            }
            users[user_id]["cars"].append(car)

    retval = list(users.values())
    return retval


class CarsSchema(NamedTuple):
    id: int
    name: str
    user_id: int


class UsersExecutor(SQLiteExecutor):
    @users_queries("SELECT * FROM users", factory=users_factory(many=True))
    async def select_all_users(self): ...

    @users_queries("SELECT * FROM users WHERE id = :id", factory=users_factory)
    async def select_user(self, id: int): ...

    @users_queries("INSERT INTO users (name) VALUES (:name)", callback=lambda c: c.lastrowid)
    async def insert_user(self, name: str) -> int: ...

    @users_queries.file('select_joined_cars.sql', callback=handle_joined_response)
    async def select_joined_cars(self): ...


class CarsExecutor(SQLiteExecutor):

    @cars_queries.file("select_cars")
    async def select_cars(self) -> list[CarsSchema]: ...

    @cars_queries("INSERT INTO cars (name, user_id) VALUES (:name, :user_id)")
    async def insert_car(self, name: str, user_id: int): ...


async def main():
    pussql = Pussql("database.db")

    async with pussql.transaction() as transaction_session:
        user_exec = UsersExecutor(transaction_session)
        cars_exec = CarsExecutor(transaction_session)

        pogger = await user_exec.insert_user('Pogger')
        await cars_exec.insert_car("Honda Civic", pogger)
        await cars_exec.insert_car("Cock & Balls", pogger)

        raise Rollback

    async with pussql.session() as session:
        user_exec = UsersExecutor(session)
        cars_exec = CarsExecutor(session)

        users = await user_exec.select_all_users()
        print('all users:', users)

        id_ = 1
        user_1 = await user_exec.select_user(id_)
        print(f'user with id {id_}:', user_1)


        cars = await cars_exec.select_cars()
        print('Cars:')
        pprint(cars)

        joined_q = await user_exec.select_joined_cars()
        print(joined_q)


if __name__ == '__main__':
    asyncio.run(main())
