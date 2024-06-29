import my_library
import pyarrow as pa
import pyarrow.dataset as pda
import pytest


@pytest.fixture
def simple_products_table():
    return pa.Table.from_pydict(
        {
            "Id": [0, 1, 2],
            "Products": [["A"], ["B"], ["C"]],
        },
    )


def test_array_agg(simple_products_table):
    ctx = my_library.get_py_context()
    ctx.register_dataset(
        name="table",
        dataset=pda.dataset(simple_products_table),
    )
    query = "SELECT * FROM 'table'"
    ctx.sql(query)
