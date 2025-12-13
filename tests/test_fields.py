# tests/test_fields.py
import pytest
from atomsql import Model, IntegerField, StringField


class User(Model):
    name = StringField(unique=True, nullable=False)
    age = IntegerField(default=18)


def test_field_validation():
    u = User(name="Alice")

    # Test valid assignment
    u.age = 25
    assert u.age == 25

    # Test invalid type
    with pytest.raises(ValueError):
        u.age = "not an int"


def test_required_fields():
    # Test missing non-nullable field
    with pytest.raises(ValueError):
        User(age=20)  # Missing 'name'
