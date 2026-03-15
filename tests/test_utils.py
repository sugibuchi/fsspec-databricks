from datetime import timedelta, timezone
from enum import Enum

import pytest

from fsspec_databricks.utils import to_datetime, value_of


def test_to_datetime():
    # 2021-01-01T00:00:00Z
    ts = 1609459200
    dt = to_datetime(ts, is_ms=False)
    assert (
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
    ) == (2021, 1, 1, 0, 0, 0)


def test_to_datetime_milliseconds():
    ts_ms = 1609459200000
    dt_ms = to_datetime(ts_ms, is_ms=True)
    assert (
        dt_ms.year,
        dt_ms.month,
        dt_ms.day,
        dt_ms.hour,
        dt_ms.minute,
        dt_ms.second,
    ) == (2021, 1, 1, 0, 0, 0)


def test_to_datetime_is_utc_aware():
    dt = to_datetime(1609459200)
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(dt) == timedelta(0)


def test_to_datetime_rfc2822d():
    dt = to_datetime("Wed, 03 Feb 2021 04:05:06 GMT")
    assert (
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
    ) == (2021, 2, 3, 4, 5, 6)
    assert dt.tzinfo == timezone.utc


class Fruit(Enum):
    Apple = 1
    Banana = 2


def test_value_of():
    assert value_of(None, Fruit) is None

    assert value_of("Apple", Fruit) == Fruit.Apple
    assert value_of("apple", Fruit) == Fruit.Apple
    assert value_of("APPLE", Fruit) == Fruit.Apple
    assert value_of(Fruit.Apple, Fruit)
    assert value_of(Fruit.Banana, Fruit) == Fruit.Banana

    with pytest.raises(ValueError):
        assert value_of("Orange", Fruit)
