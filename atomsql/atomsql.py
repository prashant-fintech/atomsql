import sqlite3


class Field:
    def __init__(self, required=True):
        self.required = required
        self.name = None

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.name)

    def __set__(self, instance, value):
        if self.required and value is None:
            raise ValueError(f"Field '{self.name}' is required and cannot be None.")
        instance.__dict__[self.name] = value

    def get_sql_type(self):
        return "TEXT"


class IntegerField(Field):
    def __set__(self, instance, value):
        if value is not None and not isinstance(value, int):
            raise TypeError(f"Field '{self.name}' expected an int, got {type(value)}.")
        super().__set__(instance, value)

    def get_sql_type(self):
        return "INTEGER"


class StringField(Field):
    def __set__(self, instance, value):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"Field '{self.name}' expected a str, got {type(value)}.")
        super().__set__(instance, value)

    def get_sql_type(self):
        return "TEXT"


class ModelMeta(type):
    """
    The Metaclass.
    Handles Schema Extraction at class definition time.
    """

    def __new__(mcs, name, bases, attrs):
        fields = {}
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, Field):
                fields[attr_name] = attr_value

        attrs["_fields"] = fields
        attrs["_table_name"] = name.lower()

        return super().__new__(mcs, name, bases, attrs)


class Model(metaclass=ModelMeta):
    """
    The Base Class.
    Handles SQL generation using the data prepared by the Metaclass.
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def save(self, cursor):
        """
        Generates and executes the INSERT statement.
        """
        table_name = self._table_name
        columns = self._fields.keys()

        values = [getattr(self, col_name) for col_name in columns]
        placeholders = ["?"] * len(columns)

        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        cursor.execute(sql, values)
        print(f"DEBUG: Saved row to table '{table_name}': {values}")


class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def register(self, model_cls):
        """
        Generates and executes the CREATE TABLE statement.
        """
        table_name = model_cls._table_name
        fields_definitions = []

        for name, field in model_cls._fields.items():
            field_type = field.get_sql_type()
            constraints = []
            if field.required:
                constraints.append("NOT NULL")

            fields_definitions.append(f"{name} {field_type} {' '.join(constraints)}")
        sql = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields_definitions)})"
        )

        self.cursor.execute(sql)
        self.conn.commit()
        print(f"DEBUG: Registered table '{table_name}' with SQL: {sql}")

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
