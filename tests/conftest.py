# tests/conftest.py
import pytest
from atomsql import Database


@pytest.fixture
def db():
    """Returns an in-memory database instance for testing."""
    database = Database("sqlite:///:memory:")
    yield database
    database.close()
