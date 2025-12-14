from atomsql import Model, IntegerField, StringField


# --- Define the Model for Testing ---
class User(Model):
    name = StringField()
    age = IntegerField()


def test_operator_overloading():
    """Test that Field operators return BinaryExpressions"""
    print("\nTesting Operator Overloading...")

    # 1. Test Equals
    expr = User.name == "Alice"
    sql, params = expr.to_sql()
    assert sql == '"name" = ?'
    assert params == ["Alice"]

    # 2. Test Greater Than
    expr = User.age > 18
    sql, params = expr.to_sql()
    assert sql == '"age" > ?'
    assert params == [18]

    # 3. Test Less Than or Equal
    expr = User.age <= 21
    sql, params = expr.to_sql()
    assert sql == '"age" <= ?'
    assert params == [21]

    print(" -> Success: All operator tests passed.")


if __name__ == "__main__":
    test_operator_overloading()
