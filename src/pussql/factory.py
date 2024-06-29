from collections import namedtuple
from dataclasses import dataclass, make_dataclass
from typing import Optional, Union, Type, Callable, TypeVar, List

from aiosqlite import Cursor

T = TypeVar('T', bound=Union[dataclass, tuple, dict])


class Factory:
    def __init__(self,
                 name: Optional[str] = 'CursorResult',
                 class_: Union[Type[dataclass], Type[namedtuple], Type[dict]] = dataclass,
                 many: bool = False,
                 many_class: Union[Type[list], Type[tuple]] = list,
                 parent: Optional['Factory'] = None):
        self.name = name
        self.class_ = class_
        self.many = many
        self.many_class = many_class
        self.function: Callable = {
            dataclass: self.convert_dataclass,
            namedtuple: self.convert_namedtuple,
            dict: self.convert_dict
        }[self.class_]
        self.parent = parent

    async def convert(self, cursor: Cursor) -> Union[T, List[T], None]:
        columns = [description[0] for description in cursor.description]
        data = await cursor.fetchone() if not self.many else await cursor.fetchall()
        if data is None:
            return
        return self.function(columns, data)

    def convert_dataclass(self, columns, data) -> Union[T, List[T]]:
        return self._unpack_data(make_dataclass(self.name, fields=columns),
                                 data)

    def convert_namedtuple(self, columns, data) -> Union[T, List[T]]:
        return self._unpack_data(make_dataclass(self.name, fields=columns),
                                 data)

    def convert_dict(self, columns, data) -> Union[T, List[T]]:
        if not self.many:
            return {k: v for k, v in zip(columns, data)}
        return [{k: v for k, v in zip(columns, row)} for row in data]

    def _unpack_data(self, class_, data):
        if not self.many:
            return class_(*data)
        return [class_(*row) for row in data]

    def __call__(self,
                 class_: Union[Type[dataclass], Type[namedtuple], Type[dict]] = None,
                 many: bool = False) -> 'Factory':
        # Override behavior for instance by creating new
        return Factory(name=self.name, class_=class_ or self.class_, many=many, many_class=self.many_class, parent=self)
