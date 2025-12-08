class Field:
    def __init__(self, required: bool = True):
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
            raise ValueError(f"Field {self.name} is required")
        instance.__dict__[self.name] = value

    def get_sql_type(self) -> str:
        return "TEXT"


class IntegerField(Field):
    def __set__(self, instance, value):
        if value is not None and not isinstance(value, int):
            raise ValueError(f"Field '{self.name}' expected an int, got {type(value)}.")
        super().__set__(instance, value)

    def get_sql_type(self) -> str:
        return "INTEGER"


class StringField(Field):
    def __set__(self, instance, value):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"Field '{self.name}' expected a str, got {type(value)}.")
        super().__set__(instance, value)

    def get_sql_type(self) -> str:
        return "TEXT"
