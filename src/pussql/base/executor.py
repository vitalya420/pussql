from abc import ABC
from typing import List

from pussql.session import Session


class BaseExecutor(ABC):
    BEGIN = 'BEGIN'
    COMMIT = 'COMMIT'
    ROLLBACK = 'ROLLBACK'

    _instances: List['BaseExecutor'] = []

    def __init__(self, session: Session):
        session.executor = self
        self.session = session
        self.__class__._instances.append(self)

    @classmethod
    def get_all_instances(cls) -> List['BaseExecutor']:
        return cls._instances
