from typing import Type, List, Any, Optional


class Query:
    def __init__(self, model_cls: Type, db):
        self.model_cls = model_cls
        self.db = db
        self._filters = {}
        self._order_by: Optional[str] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def filter(self, **kwargs: Any) -> "Query":
        self._filters.update(kwargs)
        return self

    def order_by(self, field_name: str) -> "Query":
        self._order_by = field_name
        return self

    def limit(self, limit: int) -> "Query":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "Query":
        self._offset = offset
        return self

    def _build_sql(self):
        table_name = f'"{self.model_cls._table_name}"'
        columns = [f'"{col}"' for col in self.model_cls._fields.keys()]

        sql = f"SELECT {', '.join(columns)} FROM {table_name}"
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

    def all(self) -> List[Any]:
        sql, params = self._build_sql()
        cursor = self.db.backend.execute(sql, params)
        rows = cursor.fetchall()
        results = []
        field_names = list(self.model_cls._fields.keys())
        for row in rows:
            data = dict(zip(field_names, row))
            results.append(self.model_cls(**data))
        return results

    def first(self) -> Optional[Any]:
        res = self.limit(1).all()
        return res[0] if res else None
