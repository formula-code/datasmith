import pickle
import sqlite3

from datasmith.scraping.utils import cache_completion


@cache_completion("tests/test_cache.db", "test_cache")
def helper(input_value: str) -> str:
    return input_value * 2


def test_cache_completion():
    result1 = helper("test")
    assert result1 == "testtest"

    conn = sqlite3.connect("tests/test_cache.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT function_name, argument_blob, result_blob FROM test_cache WHERE function_name = ?", ("helper",)
    )
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    function_name, argument_blob, result_blob = row

    # Deserialize argument_blob and result_blob
    args = pickle.loads(argument_blob)  # noqa: S301
    result = pickle.loads(result_blob)  # noqa: S301

    assert function_name == "helper"
    assert args == ("helper", ("test",), {})
    assert result == "testtest"
