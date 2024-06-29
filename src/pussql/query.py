import inspect
import types
from functools import wraps
from typing import Optional, Sequence, get_type_hints, Union

from pussql.base.executor import BaseExecutor
from pussql.exception import QueryFileNotFound, PussqlError, RecordNotFound
from pussql.factory import Factory
from pussql.utils import load_folder_queries, is_sync_function


class Query:
    _instances = {}

    def __new__(cls, folder='queries', file_extension='.sql'):
        if folder not in cls._instances:
            cls._instances[folder] = super().__new__(cls)
        return cls._instances[folder]

    def __init__(self, folder='queries', file_extension='.sql'):
        if getattr(self, '_initialized', False):
            return
        self.folder = folder
        self.file_extension = file_extension
        self.queries = load_folder_queries(self.folder, self.file_extension)
        self._initialized = True

    async def _execute_sql(self, sql: str, func, callback, factory: Optional[Factory], *args, **kwargs):
        executor = args[0]
        if not isinstance(executor, BaseExecutor):
            raise ValueError("Not executor")

        sig = inspect.signature(func)

        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        params = bound_args.arguments

        params.pop('self', None)

        cursor = await executor.session.execute(sql, params)

        if callback and factory:
            raise PussqlError('Set callback for factory')

        if callback:
            if is_sync_function(callback):
                return callback(cursor)
            return await callback(cursor)
        if factory:
            return await factory.convert(cursor)

        type_hints = get_type_hints(func)
        return_type = type_hints.get('return')

        if return_type is None:
            return None

        none_allowed = getattr(return_type, "__origin__", None) == Union and types.NoneType in return_type.__args__

        if getattr(return_type, "__origin__", None) == Union:
            expected_types = tuple(type_ for type_ in return_type.__args__ if type_ is not types.NoneType)
            if len(expected_types) > 1:
                raise RecordNotFound("Can not decide which type to return")
            expected_type, = expected_types
        else:
            expected_type = return_type

        if issubclass(expected_type, tuple) and hasattr(expected_type, '_fields'):
            result = await cursor.fetchone()
            result = expected_type(*result) if result else None
        elif hasattr(expected_type, '__origin__') and issubclass(expected_type.__origin__, Sequence):
            item_type = expected_type.__args__[0]
            if issubclass(item_type, tuple) and hasattr(item_type, '_fields'):
                rows = await cursor.fetchall()
                result = expected_type.__origin__(item_type(*item) for item in rows)
            else:
                raise RecordNotFound("Invalid list item type specified")

        if result is None and not none_allowed:
            raise RecordNotFound
        return result

    def file(self, path: str, callback=None, factory: Optional[Factory] = None):
        if path.endswith(self.file_extension):
            path = path.removesuffix(self.file_extension)

        if path not in self.queries:
            raise QueryFileNotFound(f"{path} not in queries")

        def wrapper(func):
            sql = self.queries[path]

            @wraps(func)
            async def decorator(*args, **kwargs):
                return await self._execute_sql(sql, func, callback, factory, *args, **kwargs)

            return decorator

        return wrapper

    def __call__(self, sql: str, callback=None, factory: Optional[Factory] = None):
        def wrapper(func):
            @wraps(func)
            async def decorator(*args, **kwargs):
                return await self._execute_sql(sql, func, callback, factory, *args, **kwargs)

            return decorator

        return wrapper
