from urllib.parse import urlparse
import logging
from .backends.base import DatabaseBackend
from .backends.sqlite import SQLiteBackend
from .backends.postgres import PostgresBackend
from .exceptions import ImproperlyConfigured
from .models import ModelMeta

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, connection_uri: str):
        self.uri = connection_uri
        self.parsed_uri = urlparse(connection_uri)
        self.backend: DatabaseBackend = self._get_backend()
        self.backend.connect()

    def _get_backend(self) -> DatabaseBackend:
        scheme = self.parsed_uri.scheme

        match scheme:
            case "sqlite":
                path = self.parsed_uri.path
                if path.startswith("/"):
                    path = path[1:]
                return SQLiteBackend(path)
            case "postgres" | "postgresql":
                return PostgresBackend(self.uri)

            case _:
                raise ImproperlyConfigured(f"Unsupported database scheme:{scheme}")

    def register(self, model_cls):
        table_name = f'"{model_cls._table_name}"'
        fields_definitions = []

        for name, field in model_cls._fields.items():
            field_type = field.get_sql_type()

            constraints = []
            if not field.nullable:
                constraints.append("NOT NULL")
            if field.unique:
                constraints.append("UNIQUE")

            definition = f'"{name}" {field_type}'
            if constraints:
                definition += f" {' '.join(constraints)}"
            fields_definitions.append(definition)

        sql = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields_definitions)})"
        )
        self.backend.execute(sql)
        self.backend.commit()
        logger.info(f"Registered model {model_cls.__name__} to table {table_name}")

    def create_all(self):
        for model_cls in ModelMeta.models:
            self.register(model_cls)
            print(f"Registered model {model_cls.__name__}")

    def execute(self, query: str, params=None):
        return self.backend.execute(query, params)

    def commit(self):
        self.backend.commit()

    def close(self):
        self.backend.close()

    def query(self, model_cls):
        from .query import Query

        return Query(model_cls, self)
