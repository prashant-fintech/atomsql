from abc import ABC, abstractmethod
from typing import Any, List, Optional


class DatabaseBackend(ABC):
    @property
    @abstractmethod
    def placeholder_char(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def connect(self, **kwargs: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: str, params: Optional[List] = None) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
