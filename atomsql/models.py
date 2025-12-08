from .fields import Field
import logging

logger = logging.getLogger(__name__)


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        fields = {}
        for key, value in list(attrs.items()):
            if isinstance(value, Field):
                fields[key] = value
        attrs["_fields"] = fields
        attrs["_table_name"] = name.lower()
        return super().__new__(cls, name, bases, attrs)


class Model(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if key in self._fields:
                setattr(self, key, value)

    def save(self, db_interface):
        table_name = f"'{self._table_name}'"
        raw_columns = list(self._fields.keys())

        values = [getattr(self, column) for column in raw_columns]
        # Note: This assumes SQLite '?' syntax
        # Future TODO: Ask db_interface for placeholder style
        placeholders = ["?" for _ in values]

        sql = f"INSERT INTO {table_name} ({', '.join(raw_columns)}) VALUES ({', '.join(placeholders)})"

        db_interface.execute(sql, values)
        logger.info(f"Saved {self._table_name} with values: {values}")
