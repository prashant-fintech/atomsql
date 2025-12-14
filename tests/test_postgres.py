# test_postgres.py
import pytest
import psycopg
from atomsql import Database, Model, StringField, IntegerField


# --- Schema Definition ---
class User(Model):
    name = StringField()
    age = IntegerField()


# --- Test Configuration ---
DB_URI = "postgresql://atomsql_user:yourpassword@localhost:5432/atomsql_test"


def test_postgres_integration():
    print("\n--- Testing PostgreSQL Integration ---")

    try:
        # 1. Connect
        db = Database(DB_URI)
        print("✅ Connected to Postgres")

        # 2. Cleanup (Drop table if exists to start fresh)
        db.execute('DROP TABLE IF EXISTS "user"')
        db.commit()

        # 3. Register Table (CREATE TABLE)
        db.register(User)
        print("✅ Table 'user' created")

        # 4. Insert Data (INSERT)
        u = User(name="Alice", age=30)
        u.save(db)
        db.commit()
        print("✅ Data saved")

        # 5. Verify (SELECT)
        db.backend.cursor.execute('SELECT "name", "age" FROM "user"')
        row = db.backend.cursor.fetchone()

        assert row is not None
        assert row[0] == "Alice"
        assert row[1] == 30
        print(f"✅ Verified Data: {row}")

        db.close()

    except psycopg.OperationalError:
        pytest.fail("❌ Could not connect to Postgres.")
    except Exception as e:
        pytest.fail(f"❌ Test Failed: {e}")


if __name__ == "__main__":
    test_postgres_integration()
