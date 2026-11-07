def read_mode(mode: str) -> bool:
    """Return True if the given file mode allows reading from the file, False otherwise."""
    return "r" in mode or "+" in mode


def write_mode(mode: str) -> bool:
    """Return True if the given file mode allows writing to the file, False otherwise."""
    return "w" in mode or "a" in mode or "x" in mode or "+" in mode


__all__ = [
    "read_mode",
    "write_mode",
]
