import pytest
from unittest.mock import patch
from atomsql import Database, Model, StringField, IntegerField


# --- Test Models ---
class Product(Model):
    name = StringField()
    price = IntegerField()
    category = StringField()


@pytest.fixture
def db():
    """Provides a fresh in-memory database and registers the Product model."""
    database = Database("sqlite:///:memory:")
    database.register(Product)
    return database


@pytest.fixture
def sample_data(db):
    """Populates the database with initial test products."""
    p1 = Product(name="Laptop", price=1200, category="Electronics")
    p2 = Product(name="Smartphone", price=800, category="Electronics")
    p3 = Product(name="Coffee Mug", price=15, category="Kitchen")

    # Use the existing save method which uses db.backend (sqlite)
    p1.save(db)
    p2.save(db)
    p3.save(db)
    db.commit()
    return [p1, p2, p3]


def test_queryset_is_lazy(db):
    """
    Ensures that calling .filter() does not trigger a database execution.
    """
    # We patch the database's execute method to see if it's called
    with patch.object(db, "execute", wraps=db.execute) as mocked_execute:
        # Define the query
        query = Product.filter(category="Electronics")

        # Check that execute has NOT been called yet
        assert (
            mocked_execute.call_count == 0
        ), "Database executed prematurely during .filter()!"

        # Adding more filters should still be lazy
        query = query.filter(price=1200)
        assert (
            mocked_execute.call_count == 0
        ), "Database executed prematurely during chained .filter()!"


def test_queryset_execution_on_iteration(db, sample_data):
    """
    Ensures that iterating over the QuerySet triggers the database call and returns models.
    """
    query = Product.filter(category="Electronics")

    # The moment we convert to a list or loop, it should execute
    results = list(query)

    assert len(results) == 2
    assert all(isinstance(p, Product) for p in results)
    assert results[0].name in ["Laptop", "Smartphone"]
    assert results[1].name in ["Laptop", "Smartphone"]


def test_queryset_generator_behavior(db, sample_data):
    """
    Verifies that the QuerySet uses a generator (yields results)
    rather than loading everything into a list internally first.
    """
    query = Product.all()
    iterator = iter(query)

    # Fetch only one item
    first_item = next(iterator)
    assert isinstance(first_item, Product)

    # The rest of the items should still be available in the generator
    remaining = list(iterator)
    assert len(remaining) == 2


def test_multiple_filters_sql_generation(db):
    """
    Tests that multiple filters are correctly combined into a WHERE clause.
    """
    query = Product.filter(category="Electronics", price=1200)
    sql, params = query._build_sql()

    # Check SQL structure
    assert "SELECT" in sql
    assert 'FROM "product"' in sql
    assert "WHERE" in sql
    assert '"category" = ?' in sql
    assert '"price" = ?' in sql
    assert params == ["Electronics", 1200]


def test_queryset_no_results(db):
    """
    Verifies behavior when no records match the query.
    """
    query = Product.filter(category="NonExistent")
    results = list(query)

    assert results == []


def test_model_not_registered_error():
    """
    Tests that calling .all() or .filter() on an unregistered model raises a RuntimeError.
    """

    class UnregisteredModel(Model):
        name = StringField()

    with pytest.raises(RuntimeError, match="not registered"):
        UnregisteredModel.all()
