"""Helpers shared by the test files: comparisons, and the work directories on each backend."""

import contextlib
import inspect
import logging
import os
from datetime import datetime
from hashlib import sha256
from io import BytesIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ExportFormat

log = logging.getLogger(__name__)


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


### Work directories

default_volume_test_root = "/Volumes/fsspec_databricks_test/fsspec_dbx_test/test/local"
default_workspace_test_root = "/fsspec-databricks-test/local"
default_dbfs_test_root = "/fsspec-databricks-test/local"


def _test_root(variable: str, default: str) -> str:
    return "/" + os.getenv(variable, default).strip("/")


def _test_dir(test_root: str, fs_class: type | None) -> str:
    """A directory named after the calling test, and the file system class it tests."""
    name = inspect.stack()[2].function
    return (
        f"{test_root}/{name}_{fs_class.__name__}" if fs_class else f"{test_root}/{name}"
    )


def _rm_volume_dir(client: WorkspaceClient, path: str) -> None:
    """Delete a volume directory and all in it, with the Files API, which every workspace serves."""
    for i in client.files.list_directory_contents(path):
        if i.is_directory:
            _rm_volume_dir(client, i.path.rstrip("/"))
        else:
            client.files.delete(i.path)
    client.files.delete_directory(path)


def prepare_volume_test_root(client: WorkspaceClient) -> str:
    """Create the Volume work directory, or empty it."""
    path = _test_root("FSSPEC_DATABRICKS_VOLUME_TEST_ROOT", default_volume_test_root)
    log.info("Preparing volume test root: %s", path)
    catalog, schema, volume = path.split("/")[2:5]
    # The volume must exist: the tests never create one.
    client.volumes.read(f"{catalog}.{schema}.{volume}")
    with contextlib.suppress(NotFound):
        _rm_volume_dir(client, path)
    client.files.create_directory(path)
    return path


def prepare_workspace_test_root(client: WorkspaceClient) -> str:
    """Create the Workspace work directory, or empty it."""
    path = _test_root(
        "FSSPEC_DATABRICKS_WORKSPACE_TEST_ROOT", default_workspace_test_root
    )
    log.info("Preparing workspace test root: %s", path)
    try:
        client.workspace.get_status(path)
    except NotFound:
        client.workspace.mkdirs(path)
        return path
    for i in client.workspace.list(path):
        client.workspace.delete(i.path, recursive=True)
    return path


def prepare_dbfs_test_root(client: WorkspaceClient) -> str:
    """Create the legacy DBFS work directory, or empty it."""
    path = _test_root("FSSPEC_DATABRICKS_DBFS_TEST_ROOT", default_dbfs_test_root)
    log.info("Preparing DBFS test root: %s", path)
    client.dbfs.mkdirs(path)
    for i in client.dbfs.list(path):
        client.dbfs.delete(i.path, recursive=True)
    return path


def legacy_dbfs_disabled(client: WorkspaceClient) -> bool:
    """Whether the workspace disables the legacy DBFS API."""
    return client.workspace_settings_v2.get_public_workspace_setting(
        "disable_legacy_dbfs"
    ).effective_boolean_val.value


def init_volume_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type | None = None,
    items: dict[str, dict | bytes] | None = None,
) -> str:
    """An empty volume directory named after the calling test, holding the given files and directories."""
    test_dir = _test_dir(test_root, fs_class)
    with contextlib.suppress(NotFound):
        _rm_volume_dir(client, test_dir)
    client.files.create_directory(test_dir)

    def init(path: str, _items: dict[str, dict | bytes]):
        for n, i in _items.items():
            if isinstance(i, dict):
                client.files.create_directory(f"{path}/{n}")
                init(f"{path}/{n}", i)
            else:
                client.files.upload(f"{path}/{n}", BytesIO(i))

    init(test_dir, items or {})
    return test_dir


def init_workspace_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type | None = None,
    items: dict[str, dict | tuple] | None = None,
) -> str:
    """An empty Workspace directory named after the calling test, holding the given items.

    A file is a tuple of its content, import format and language.
    """
    test_dir = _test_dir(test_root, fs_class)
    with contextlib.suppress(NotFound):
        client.workspace.delete(test_dir, recursive=True)
    client.workspace.mkdirs(test_dir)

    def init(path: str, _items: dict[str, dict | tuple]):
        for n, i in _items.items():
            if isinstance(i, dict):
                client.workspace.mkdirs(f"{path}/{n}")
                init(f"{path}/{n}", i)
            else:
                content, fmt, language = i
                client.workspace.upload(
                    f"{path}/{n}", content, format=fmt, language=language
                )

    init(test_dir, items or {})
    return test_dir


def init_dbfs_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type | None = None,
    items: dict[str, dict | bytes] | None = None,
) -> str:
    """An empty DBFS directory named after the calling test, holding the given files and directories."""
    test_dir = _test_dir(test_root, fs_class)
    client.dbfs.delete(test_dir, recursive=True)
    client.dbfs.mkdirs(test_dir)

    def init(path: str, _items: dict[str, dict | bytes]):
        for n, i in _items.items():
            if isinstance(i, dict):
                client.dbfs.mkdirs(f"{path}/{n}")
                init(f"{path}/{n}", i)
            else:
                with client.dbfs.open(f"{path}/{n}", write=True) as f:
                    f.write(i)

    init(test_dir, items or {})
    return test_dir


def volume_download(client: WorkspaceClient, path: str) -> bytes:
    with client.files.download(path).contents as f:
        return f.read()


def workspace_download(
    client: WorkspaceClient, path: str, fmt: ExportFormat = ExportFormat.AUTO
) -> bytes:
    with client.workspace.download(path, format=fmt) as f:
        return f.read()


def dbfs_download(client: WorkspaceClient, path: str) -> bytes:
    with client.dbfs.download(path) as f:
        return f.read()
