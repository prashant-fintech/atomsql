from typing import Type, Any, TypeVar, TYPE_CHECKING, Iterable, Optional
import functools

if TYPE_CHECKING:
    from .models import Model
    from .db import Database

T = TypeVar("T", bound="Model")


def aggregate_method(func):
    @functools.wraps(func)
    def wrapper(self: "QuerySet[T]", *args, **kwargs) -> Any:
        agg_expression = func(self, *args, **kwargs)
        sql, params = self._build_sql(select_expression=agg_expression)
        cursor = self.db.backend.execute(sql, params)
        result = cursor.fetchone()
        return result[0] if result else None

    return wrapper


class QuerySet(Iterable[T]):
    def __init__(self, model_cls: Type[T], db: "Database"):
        self.model_cls = model_cls
        self.db = db
        self._filters = {}
        self._order_by: Optional[str] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def filter(self, **kwargs: Any) -> "QuerySet[T]":
        self._filters.update(kwargs)
        return self

    def order_by(self, field_name: str) -> "QuerySet[T]":
        self._order_by = field_name
        return self

    def limit(self, limit: int) -> "QuerySet[T]":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "QuerySet[T]":
        self._offset = offset
        return self

    @aggregate_method
    def count(self) -> str:
        return "COUNT(*)"

    @aggregate_method
    def sum(self, field_name: str) -> str:
        if field_name not in self.model_cls._fields:
            raise ValueError(
                f"Field '{field_name}' does not exist on model '{self.model_cls.__name__}'"
            )
        return f'SUM("{field_name}")'

    @aggregate_method
    def avg(self, field_name: str) -> str:
        if field_name not in self.model_cls._fields:
            raise ValueError(
                f"Field '{field_name}' does not exist on model '{self.model_cls.__name__}'"
            )
        return f'AVG("{field_name}")'

    def _build_sql(self, select_expression: Optional[str] = None):
        table_name = f'"{self.model_cls._table_name}"'

        if select_expression is None:
            select_expression = ", ".join(
                [f'"{col}"' for col in self.model_cls._fields.keys()]
            )

        sql = f"SELECT {select_expression} FROM {table_name} "

        params = []

        if self._filters:
            sql += " WHERE "
            conditions = []
            for key, value in self._filters.items():
                conditions.append(f'"{key}" = {self.db.backend.placeholder_char}')
                params.append(value)
            sql += " AND ".join(conditions)

        if self._order_by:
            direction = "DESC" if self._order_by.startswith("-") else "ASC"
            field_name = self._order_by.lstrip("-")
            sql += f' ORDER BY "{field_name}" {direction}'

        if self._limit:
            sql += f" LIMIT {self._limit}"

        if self._offset:
            sql += f" OFFSET {self._offset}"

        return sql, params

    def __iter__(self):
        sql, params = self._build_sql()
        cursor = self.db.backend.execute(sql, params)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            data = dict(zip(self.model_cls._fields.keys(), row))
            yield self.model_cls(**data)
