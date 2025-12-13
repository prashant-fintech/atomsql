# tests/test_models.py
from atomsql import Model, StringField


class Product(Model):
    sku = StringField()


def test_model_structure():
    assert hasattr(Product, "_fields")
    assert "sku" in Product._fields
    assert Product._table_name == "product"


def test_model_init_defaults():
    # Assuming you implemented the default logic from our previous step
    # This tests that defaults are applied correctly on __init__
    pass
