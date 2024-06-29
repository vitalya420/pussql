import inspect
import os
from typing import Callable


def load_folder_queries(folder: str, file_extension: str = '.sql'):
    abs_path = os.path.join(os.getcwd(), folder)
    _mapping = {}
    for file in os.listdir(abs_path):
        file_abs_path = os.path.join(abs_path, file)
        if os.path.isfile(file_abs_path) and file.endswith(file_extension):
            with open(file_abs_path, 'r') as query_file:
                _mapping[file.removesuffix(file_extension)] = query_file.read()
    return _mapping


def is_sync_function(func: Callable) -> bool:
    return inspect.isfunction(func) and not inspect.iscoroutinefunction(func)
