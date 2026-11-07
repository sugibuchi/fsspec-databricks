import email.utils
from datetime import UTC, datetime
from enum import Enum
from typing import Any, TypeVar


def to_datetime(timestamp: float | str | None, is_ms: bool = True) -> datetime | None:
    """Convert a timestamp, in milliseconds unless `is_ms` is False, or an RFC 822 date to a datetime."""

    if timestamp is None:
        return None
    elif isinstance(timestamp, (float, int)):
        if is_ms:
            return datetime.fromtimestamp(timestamp / 1000, tz=UTC)
        else:
            return datetime.fromtimestamp(timestamp, tz=UTC)
    elif isinstance(timestamp, str):
        # An RFC 822 date, like "Wed, 02 Oct 2002 08:00:00 EST"
        return email.utils.parsedate_to_datetime(timestamp)
    else:
        raise ValueError(f"Cannot convert timestamp {timestamp} to datetime")


def to_rfc3339(dt: datetime) -> str:
    """Convert a datetime object to an RFC 3339 formatted string."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


ENUM = TypeVar("ENUM", bound=Enum)


def value_of(value: Any, enum_class: type[ENUM]) -> ENUM | None:
    """Convert a value, or a member's name in any case, to a member of the enum."""
    if value is None:
        return None
    elif isinstance(value, enum_class):
        return value
    elif isinstance(value, str):
        try:
            return enum_class(value)
        except ValueError:
            # Case-insensitive match of the name
            value_of_name = {v.name.lower(): v for v in enum_class}
            if value.lower() in value_of_name:
                return value_of_name[value.lower()]

    raise ValueError(f"Cannot convert value {value} to enum {enum_class}")


def read_mode(mode: str) -> bool:
    """Return True if the given file mode allows reading from the file, False otherwise."""
    return "r" in mode or "+" in mode


def write_mode(mode: str) -> bool:
    """Return True if the given file mode allows writing to the file, False otherwise."""
    return "w" in mode or "a" in mode or "x" in mode or "+" in mode


__all__ = [
    "read_mode",
    "to_datetime",
    "to_rfc3339",
    "value_of",
    "write_mode",
]
