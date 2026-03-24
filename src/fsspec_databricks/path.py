"""Utilities for parsing Databricks file path URLs."""

import re
from enum import Enum

scheme = "dbfs"
""" The URL scheme used for Databricks file paths. """

dbfs_root = "/"
""" The root directory for DBFS file paths. """

volumes_root = "/Volumes"
""" The root directory for Unity Catalog Volumes in Databricks file paths. """

workspace_root = "/Workspace"
""" The root directory for Workspace files in Databricks file paths. """

dbfs_url_pat = re.compile(rf"(?:(?P<scheme>{scheme}):/?)?(?P<path>/(?:[^/].*)?)")
""" Regular expression pattern for matching DBFS URLs and capturing the path component. """

workspace_path_pat = re.compile(rf"{workspace_root}(?:/(?P<path>.*)|)")
""" Regular expression pattern for matching Workspace file paths and capturing the path within the workspace. """

volume_path_pat = re.compile(
    rf"{volumes_root}(?:/(?:(?P<catalog>[^/]+)(?:/(?:(?P<schema>[^/]+)(?:/(?:(?P<volume>[^/]+)(?:/(?P<path>.*)|)|)|)|)|)|)|)"
)
""" Regular expression pattern for matching Unity Catalog Volume file paths and capturing the catalog, schema, volume, and path components. """


def strip_scheme(path: str) -> str:
    """Strip the "dbfs:/" scheme from the given path if it exists.

    Parameters
    ----------
    path : str
        The path to strip the scheme from.
    """
    path = str(path)
    m = dbfs_url_pat.fullmatch(path)
    if m:
        return m.group("path")
    else:
        raise ValueError(
            f"Invalid path: {path}. Path must be a DBFS URL or an absolute POSIX path starting with '/'."
        )


def unstrip_scheme(path: str) -> str:
    """Add the "dbfs:/" scheme to the given path if it doesn't already have it.

    Parameters
    ----------
    path : str
        The path to add the scheme to.
    """
    path = str(path)
    m = dbfs_url_pat.fullmatch(path)
    if m:
        if path.startswith("dbfs:/"):
            return path
        else:
            return f"{scheme}:{path}"
    else:
        raise ValueError(
            f"Invalid path: {path}. Path must be a DBFS URL or an absolute POSIX path starting with '/'."
        )


class FileSystemType(Enum):
    """Type of Databricks File System."""

    Workspace = 1
    """ Workspace File System """
    Volume = 2
    """ Unity Catalog Volume File System """
    Legacy = 3
    """ Databricks DBFS File System (Legacy) """


def fs_type(path: str) -> FileSystemType:
    """Returns the type of filesystem for the given path.

    Parameters
    ----------
    path : str
        The path (DBFS URL or POSIX path) to check.

    Returns
    -------
    FileSystemType
        The type of filesystem (Volumes, Workspace, or DBFS).
    """
    stripped = strip_scheme(path)

    if volume_path_pat.fullmatch(stripped):
        return FileSystemType.Volume
    elif workspace_path_pat.fullmatch(stripped):
        return FileSystemType.Workspace
    elif stripped.startswith("/"):
        return FileSystemType.Legacy
    else:
        raise ValueError(
            f"Invalid path: {path}. Path must be a DBFS URL or an absolute POSIX path starting with '/'."
        )


def parse_volume_path(
    path: str,
) -> tuple[str | None, str | None, str | None, str | None, str]:
    """Parse a Unity Catalog Volume file path (DBFS URL or POSIX path) and return its components.

    Parameters
    ----------
    path : str
        The Unity Catalog Volume file path (DBFS URL or POSIX path) to parse.

    Returns
    -------
    tuple[str, str, str, str, str]
        A tuple containing the catalog name, schema name, volume name, local path within the volume, and full absolute path.
        If a components is not present in the path, its corresponding value in the tuple will be None.

        - Example: `dbfs:/Volumes/catalog_a/schema_b` returns `("catalog_a", "schema_b", None, None, "/Volumes/catalog_a/schema_b")`

        Returned paths are guaranteed to start with a slash ("/").
    """
    stripped = strip_scheme(path)
    m = volume_path_pat.fullmatch(stripped)
    if not m:
        raise ValueError(f"Invalid Unity Catalog Volume URL: {path}")

    catalog = m.group("catalog") or None
    schema = m.group("schema") or None
    volume = m.group("volume") or None
    path = f"/{m.group('path') or ''}" if volume else None
    return catalog, schema, volume, path, stripped


def parse_workspace_path(path: str) -> str:
    """Parse a Workspace file path (DBFS URL or POSIX path) and return the path within the workspace.

    Parameters
    ----------
    path : str
        The Workspace file path (DBFS URL or POSIX path) to parse.

    Returns
    -------
    str
        The path within the workspace.
        Returned path is guaranteed to start with a slash ("/").
    """
    stripped = strip_scheme(path)
    m = workspace_path_pat.fullmatch(stripped)
    if not m:
        raise ValueError(f"Invalid Workspace file URL: {path}")

    return f"/{m.group('path') or ''}"


def parse_dbfs_path(path: str) -> str:
    """Parse a DBFS file path (DBFS URL or POSIX path) and return the only path.

    Parameters
    ----------
    path : str
        The DBFS URL to parse.

    Returns
    -------
    str
        The path within DBFS.
        Returned path is guaranteed to start with a slash ("/").
    """
    return strip_scheme(path)


__all__ = [
    "FileSystemType",
    "dbfs_root",
    "fs_type",
    "parse_dbfs_path",
    "parse_volume_path",
    "parse_workspace_path",
    "scheme",
    "strip_scheme",
    "unstrip_scheme",
    "volumes_root",
    "workspace_root",
]
