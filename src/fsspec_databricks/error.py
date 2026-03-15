import errno
import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from databricks.sdk.errors import (
    AlreadyExists,
    NotFound,
    PermissionDenied,
    ResourceAlreadyExists,
    Unauthenticated,
)


def os_error(
    exc_class: type[OSError],
    error_no: int,
    path: str,
    message: str | None = None,
) -> OSError:
    """Return an OSError for the given exception class, system error number, and path."""
    strerror = os.strerror(error_no)
    if message:
        strerror = f"{message}: {strerror}"
    return exc_class(error_no, strerror, path)


def io_error(path: str, message: str | None = None) -> OSError:
    """Return a generic IOError for the given path."""
    return os_error(OSError, errno.EIO, path, message)


def file_not_found_error(path: str, message: str | None = None) -> OSError:
    """Return a FileNotFoundError for the given path."""
    return os_error(FileNotFoundError, errno.ENOENT, path, message)


def file_exists_error(path: str, message: str | None = None) -> OSError:
    """Return a FileExistsError for the given path."""
    return os_error(FileExistsError, errno.EEXIST, path, message)


def not_a_directory_error(path: str, message: str | None = None) -> OSError:
    """Return a NotADirectoryError for the given path."""
    return os_error(NotADirectoryError, errno.ENOTDIR, path, message)


def is_a_directory_error(path: str, message: str | None = None) -> OSError:
    """Return an IsADirectoryError for the given path."""
    return os_error(IsADirectoryError, errno.EISDIR, path, message)


def permission_error(path: str, message: str | None = None) -> OSError:
    """Return a PermissionError for the given path."""
    return os_error(PermissionError, errno.EPERM, path, message)


def access_error(path: str, message: str | None = None) -> OSError:
    """Return an PermissionError for the given path."""
    return os_error(PermissionError, errno.EACCES, path, message)


def not_empty_error(path: str, message: str | None = None) -> OSError:
    """Return a DirectoryNotEmptyError for the given path."""
    return os_error(OSError, errno.ENOTEMPTY, path, message)


@contextmanager
def error_mapping(path) -> Generator[None, Any, None]:
    """Context manager to map exception classes defined by Databricks SDK to standard exceptions."""
    try:
        yield
    except NotFound as e:
        raise file_not_found_error(path) from e
    except AlreadyExists as e:
        raise file_exists_error(path) from e
    except ResourceAlreadyExists as e:
        raise file_exists_error(path) from e
    except Unauthenticated as e:
        raise access_error(path) from e
    except PermissionDenied as e:
        raise access_error(path) from e
    except:
        raise


__all__ = [
    "access_error",
    "error_mapping",
    "file_exists_error",
    "file_not_found_error",
    "io_error",
    "is_a_directory_error",
    "not_a_directory_error",
    "not_empty_error",
    "os_error",
    "permission_error",
]
