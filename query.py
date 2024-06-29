import os.path
from functools import wraps
from typing import Optional

from src.pussql.base.executor import BaseExecutor
from pussql.factory import Factory
from src.pussql.utils import is_sync_function


def load_folder_queries(folder: str, file_extension: str = '.sql'):
    abs_path = os.path.join(os.getcwd(), folder)
    _mapping = {}
    for file in os.listdir(abs_path):
        file_abs_path = os.path.join(abs_path, file)
        if os.path.isfile(file_abs_path) and file.endswith(file_extension):
            with open(file_abs_path, 'r') as query_file:
                _mapping[file.removesuffix(file_extension)] = query_file.read()
    return _mapping


class Query:
    def __init__(self, folder: str = 'queries', file_extension: str = '.sql'):
        self.folder = folder
        self.file_extension = file_extension
        self.queries = load_folder_queries(self.folder, self.file_extension)

    async def _execute_sql(self, sql: str, decorated_function, callback, factory: Optional[Factory], *args, **kwargs):
        executor = args[0]
        if not isinstance(executor, BaseExecutor):
            raise ValueError("Not executor")
        cursor = await executor.session.execute(sql)
        if callback:
            if is_sync_function(callback):
                return callback(cursor)
            return await callback(cursor)
        if factory:
            return await factory.convert(cursor)

    def file(self, path: str, callback=None, factory: Optional[Factory] = None):
        if path.endswith(self.file_extension):
            path = path.removesuffix(self.file_extension)

        if path not in self.queries:
            raise ValueError(f"{path} not in queries")

        def wrapper(func):
            sql = self.queries[path]

            @wraps(func)
            async def decorator(*args, **kwargs):
                return await self._execute_sql(sql, func, callback, factory, *args, **kwargs)

            return decorator

        return wrapper

    def __call__(self,
                 sql: str,
                 callback=None,
                 factory: Optional[Factory] = None):
        def wrapper(func):
            @wraps(func)
            async def decorator(*args, **kwargs):
                return await self._execute_sql(sql,
                                               func,
                                               callback,
                                               factory,
                                               *args,
                                               **kwargs)

            return decorator

        return wrapper


def get_default():
    query = Query()
    return query
