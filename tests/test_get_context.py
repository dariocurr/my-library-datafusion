import my_library


def test_get_context():
    context = my_library.get_py_context()
    assert context.catalog().names() == ["public"]
