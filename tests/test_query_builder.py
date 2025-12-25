import pytest
from atomsql import Database, Model, StringField, IntegerField


# 1. Define a Model for testing purposes
class User(Model):
    name = StringField()
    age = IntegerField()


# 2. Use a pytest fixture for database setup
@pytest.fixture
def db():
    """Provides a fresh in-memory SQLite database for each test."""
    # Using the URI format established in atomsql/db.py
    database = Database("sqlite:///:memory:")
    database.register(User)
    return database


# 3. The Query Builder Test Case
def test_query_builder(db):
    """
    Test that the Query Builder correctly handles method chaining,
    filtering, ordering, and limits.
    """
    # Insert test data using the model's save method
    User(name="Alice", age=25).save(db)
    User(name="Bob", age=30).save(db)
    User(name="Charlie", age=35).save(db)

    # --- Test Filtering and Chaining ---
    # Accesses the query builder via the .objects() entry point
    query_results = User.objects(db).filter(name="Alice").all()
    assert len(query_results) == 1
    assert query_results[0].name == "Alice"
    assert query_results[0].age == 25

    # --- Test Order By and Limit ---
    # This assumes the order_by method correctly parses the '-' prefix
    # to apply DESC ordering in the generated SQL.
    results = User.objects(db).order_by("-age").limit(2).all()

    assert len(results) == 2
    assert results[0].name == "Charlie"  # Age 35
    assert results[1].name == "Bob"  # Age 30


def test_query_empty_results(db):
    """Verify that queries return an empty list when no matches are found."""
    results = User.objects(db).filter(name="NonExistent").all()
    assert results == []


def test_query_first(db):
    """Test the .first() helper method."""
    User(name="Alice", age=25).save(db)
    user = User.objects(db).filter(name="Alice").first()
    assert user is not None
    assert user.name == "Alice"
