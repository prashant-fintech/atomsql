import pytest
from atomsql import Database, Model, StringField, IntegerField


class Expense(Model):
    category = StringField()
    amount = IntegerField()


@pytest.fixture
def db():
    database = Database("sqlite:///:memory:")
    database.register(Expense)
    # Seed data
    Expense(category="Food", amount=50).save(database)
    Expense(category="Food", amount=30).save(database)
    Expense(category="Rent", amount=1000).save(database)
    database.commit()
    return database


def test_count(db):
    assert Expense.objects().count() == 3
    assert Expense.objects().filter(category="Food").count() == 2


def test_sum(db):
    total = Expense.objects().filter(category="Food").sum("amount")
    assert total == 80


def test_avg(db):
    average = Expense.objects().filter(category="Food").avg("amount")
    assert average == 40.0


def test_invalid_field_aggregation(db):
    with pytest.raises(ValueError):
        Expense.objects().sum("non_existent_field")
