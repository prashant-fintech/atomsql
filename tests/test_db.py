from atomsql import Model, StringField, IntegerField


class Person(Model):
    name = StringField()
    age = IntegerField()


def test_table_creation(db):
    db.register(Person)
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='person';"
    )
    assert cursor.fetchone() is not None


def test_insert_and_select(db):
    db.register(Person)
    p = Person(name="Bob", age=30)
    p.save(db)

    cursor = db.execute("SELECT name, age FROM person")
    row = cursor.fetchone()
    assert row == ("Bob", 30)
