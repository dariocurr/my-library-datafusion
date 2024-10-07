import my_library
import pyarrow as pa
import pyarrow.dataset as pda
import pytest


@pytest.fixture
def simple_values_arrow():
    return pa.Table.from_pydict(
        {
            "id": [0, 1, 2],
            "values": [1.0, 2.0, 3.0],
        },
    )


def test_udf(simple_values_arrow: pa.Table):
    ctx = my_library.get_py_context()
    name = "table"
    ctx.register_dataset(
        name=name,
        dataset=pda.dataset(simple_values_arrow),
    )
    query = f"SELECT add_42(values) FROM '{name}'"
    result = ctx.sql(query)
    assert result
