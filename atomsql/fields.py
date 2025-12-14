from typing import Any, Tuple, List


class BinaryExpression:
    def __init__(self, field: "Field", operator: str, value: Any):
        self.field = field
        self.operator = operator
        self.value = value

    def to_sql(self) -> Tuple[str, List[Any]]:
        sql = f'"{self.field.name}" {self.operator} ?'
        return sql, [self.value]

    def __repr__(self):
        return f"<BinaryExpression: {self.field.name} {self.operator} {self.value}>"


class Field:
    def __init__(
        self, default: Any = None, unique: bool = False, nullable: bool = True
    ):
        if not isinstance(unique, bool):
            raise TypeError("Option 'unique' must be a boolean")
        if not isinstance(nullable, bool):
            raise TypeError("Option 'nullable' must be a boolean")

        self.default = default
        self.unique = unique
        self.nullable = nullable
        self.name = None

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.name)

    def __set__(self, instance, value):
        if value is None:
            if not self.nullable:
                raise ValueError(f"Field '{self.name}' cannot be None (nullable=False)")
            instance.__dict__[self.name] = None
            return
        self.validate_type(value)
        instance.__dict__[self.name] = value

    def validate_type(self, value):
        raise NotImplementedError

    def get_sql_type(self) -> str:
        return "TEXT"

    def __eq__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, "=", value)

    def __ne__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, "!=", value)

    def __lt__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, "<", value)

    def __le__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, "<=", value)

    def __gt__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, ">", value)

    def __ge__(self, value: Any) -> BinaryExpression:
        return BinaryExpression(self, ">=", value)


class IntegerField(Field):
    def validate_type(self, value):
        if not isinstance(value, int):
            raise ValueError(f"Field '{self.name}' expected an int, got {type(value)}.")

    def get_sql_type(self) -> str:
        return "INTEGER"


class StringField(Field):
    def validate_type(self, value):
        if not isinstance(value, str):
            raise ValueError(f"Field '{self.name}' expected a str, got {type(value)}.")

    def get_sql_type(self) -> str:
        return "TEXT"
