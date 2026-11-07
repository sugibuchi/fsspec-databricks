import pytest

from fsspec_databricks.file import read_mode, write_mode


@pytest.mark.parametrize(
    ("mode", "readable", "writable"),
    [
        ("rb", True, False),
        ("wb", False, True),
        ("ab", False, True),
        ("xb", False, True),
        ("r+b", True, True),
    ],
)
def test_modes(mode: str, readable: bool, writable: bool):
    assert read_mode(mode) is readable
    assert write_mode(mode) is writable
