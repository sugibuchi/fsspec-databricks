from datetime import datetime
from hashlib import sha256


def datetime_eq(a: datetime, b: datetime):
    """Compares two datetimes for equality, ignoring microseconds."""
    if a is None:
        return b is None
    else:
        return a.replace(microsecond=0) == b.replace(microsecond=0)


def bytes_sig(data: bytes) -> tuple[int, str]:
    """Returns a tuple of the length and SHA256 hash of the given bytes data, for comparison of long byte arrays."""
    h = sha256()
    h.update(data)
    return len(data), h.hexdigest()
