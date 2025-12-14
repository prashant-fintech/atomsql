import sqlite3
from typing import Any, List, Optional
from .base import DatabaseBackend


class SQLiteBackend(DatabaseBackend):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def connect(self, **kwargs: Any) -> None:
        target = self.db_path if self.db_path else ":memory:"
        self.connection = sqlite3.connect(target, **kwargs)
        self.cursor = self.connection.cursor()

    def disconnect(self) -> None:
        self.connection.close()

    def execute(self, query: str, params: Optional[List] = None) -> Any:
        if params is None:
            params = []
        return self.cursor.execute(query, params)

    def commit(self) -> None:
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    @property
    def placeholder_char(self) -> str:
        return "?"
