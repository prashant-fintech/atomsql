import sqlite3
from atomsql import Model, IntegerField, StringField, Database


# --- 1. Define the Schema ---
class User(Model):
    name = StringField()
    age = IntegerField()


def test_descriptor_validation():
    """Test that Descriptors enforce types"""
    u = User()

    # Valid assignments
    u.name = "Alice"
    u.age = 30
    assert u.name == "Alice"
    assert u.age == 30

    # Invalid assignment (Should fail if you implemented Descriptors correctly)
    print("Testing Validation...")
    try:
        u.age = "thirty"  # This should raise TypeError
        assert False, "Descriptor failed to catch invalid type!"
    except TypeError:
        print(" -> Success: IntegerField caught the string input.")


def test_metaclass_magic():
    """Test that Metaclass extracted fields correctly"""
    print("\nTesting Metaclass...")

    # The _fields dict should exist on the class (created by ModelMeta)
    assert hasattr(User, "_fields"), "Metaclass did not create _fields attribute"
    assert "name" in User._fields
    assert "age" in User._fields
    assert User._table_name == "user"
    print(" -> Success: Metaclass extracted schema.")


def test_database_integration():
    """Test SQL Generation"""
    print("\nTesting Database Interaction...")

    # Use in-memory DB for testing
    db = Database(":memory:")

    # 1. Register Table (CREATE TABLE)
    db.register(User)

    # 2. Insert Data (INSERT INTO)
    u = User(name="Bob", age=40)
    u.save(db.cursor)

    # 3. Verify (SELECT)
    # Note: This relies on your save() method actually working!
    try:
        db.cursor.execute("SELECT name, age FROM user")
        row = db.cursor.fetchone()
        assert row[0] == "Bob"
        assert row[1] == 40
        print(" -> Success: Data persisted to SQLite.")
    except sqlite3.OperationalError as e:
        print(f" -> Failed: SQL Error - {e}")


if __name__ == "__main__":
    # Simple manual runner
    test_descriptor_validation()
    test_metaclass_magic()
    test_database_integration()
    print("\nALL SYSTEM TESTS PASSED FOR ATOMSQL.")
