from .fields import Field
import logging

logger = logging.getLogger(__name__)


class ModelMeta(type):
    models = []

    def __new__(cls, name, bases, attrs):
        fields = {}
        for key, value in list(attrs.items()):
            if isinstance(value, Field):
                fields[key] = value
        attrs["_fields"] = fields
        attrs["_table_name"] = name.lower()
        new_class = super().__new__(cls, name, bases, attrs)

        if bases:
            cls.models.append(new_class)

        return new_class


class Model(metaclass=ModelMeta):
    def __init__(self, **kwargs):
        for name, field in self._fields.items():
            if name in kwargs:
                value = kwargs[name]
            else:
                value = field.default
                if callable(value):
                    value = value()
            setattr(self, name, value)

    def save(self, db_interface):
        table_name = f'"{self._table_name}"'
        raw_columns = list(self._fields.keys())
        values = [getattr(self, column) for column in raw_columns]
        placeholders = [db_interface.backend.placeholder_char for _ in values]

        sql = f"INSERT INTO {table_name} ({', '.join(raw_columns)}) VALUES ({', '.join(placeholders)})"

        db_interface.execute(sql, values)
        logger.info(f"Saved {self._table_name} with values: {values}")
