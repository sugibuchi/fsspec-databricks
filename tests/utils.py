from hashlib import sha256


def bytes_sig(data: bytes) -> tuple[int, str]:
    """Returns a tuple of the length and SHA256 hash of the given bytes data, for comparison of long byte arrays."""
    h = sha256()
    h.update(data)
    return len(data), h.hexdigest()
