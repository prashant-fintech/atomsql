from typing import Any, List, Optional, TYPE_CHECKING
from .base import DatabaseBackend

if TYPE_CHECKING:
    import psycopg


class PostgresBackend(DatabaseBackend):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection: Optional["psycopg.Connection"] = None
        self.cursor: Optional["psycopg.Cursor"] = None

    def connect(self, **kwargs: Any) -> None:
        import psycopg

        self.connection = psycopg.connect(self.db_path, **kwargs)
        self.cursor = self.connection.cursor()

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()

    def execute(self, query: str, params: Optional[List] = None) -> Any:
        if params is None:
            params = []
        return self.cursor.execute(query, params)

    def commit(self) -> None:
        if self.connection:
            self.connection.commit()

    def close(self) -> None:
        self.disconnect()

    @property
    def placeholder_char(self) -> str:
        return "%s"
