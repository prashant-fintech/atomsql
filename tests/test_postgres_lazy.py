import sys
import unittest
from unittest.mock import patch, MagicMock
from atomsql.backends.postgres import PostgresBackend


class TestPostgresLazyImport(unittest.TestCase):
    def setUp(self):
        # Remove psycopg from sys.modules if it exists to simulate it not being imported
        if "psycopg" in sys.modules:
            del sys.modules["psycopg"]

    def test_instantiation_without_psycopg(self):
        """Test that PostgresBackend can be instantiated without psycopg installed/imported"""
        with patch.dict(sys.modules, {"psycopg": None}):
            try:
                backend = PostgresBackend(
                    db_path="postgresql://atomsql_user:yourpassword@localhost:5432/atomsql_test"
                )
                self.assertIsInstance(backend, PostgresBackend)
            except ImportError:
                self.fail("PostgresBackend raised ImportError on instantiation")

    def test_connect_imports_psycopg(self):
        """Test that connect() triggers the import of psycopg"""
        # Mock psycopg module
        mock_psycopg = MagicMock()

        with patch.dict(sys.modules, {"psycopg": mock_psycopg}):
            backend = PostgresBackend(
                db_path="postgresql://atomsql_user:yourpassword@localhost:5432/atomsql_test"
            )
            backend.connect()

            # Check if connect was called on the mock
            mock_psycopg.connect.assert_called_once()


if __name__ == "__main__":
    unittest.main()
