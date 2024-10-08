import my_library
import pyarrow as pa
import pyarrow.dataset as pda
import pytest


@pytest.fixture
def simple_products_table():
    return pa.Table.from_pydict(
        {
            "id": [0, 1, 2],
            "products": [["A"], ["B"], ["C"]],
        },
    )


def test_array_agg(simple_products_table: pa.Table):
    ctx = my_library.get_py_context()
    name = "table"
    ctx.register_dataset(
        name=name,
        dataset=pda.dataset(simple_products_table),
    )
    query = f"SELECT * FROM '{name}'"
    result = ctx.sql(query)
    assert result
