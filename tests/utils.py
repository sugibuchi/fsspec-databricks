import logging
import os
import posixpath
from datetime import datetime
from hashlib import sha256
from io import BytesIO
from unittest.mock import MagicMock

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
    ObjectInfo,
    ObjectType,
)

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


class FakeWorkspaceAPI:
    """An in-memory `WorkspaceClient.workspace`, answering as the Workspace API does, recording its calls."""

    def __init__(self):
        self.objects: dict[str, ObjectInfo] = {
            "/": ObjectInfo(path="/", object_type=ObjectType.DIRECTORY, object_id=0)
        }
        self.data: dict[str, bytes] = {}
        self.calls: list[tuple] = []
        self._next_id = 1

    def _status(self, path: str) -> ObjectInfo:
        if path not in self.objects:
            raise ResourceDoesNotExist(f"Path ({path}) doesn't exist.")
        return self.objects[path]

    def _children(self, path: str) -> list[str]:
        return sorted(
            p for p in self.objects if p != "/" and posixpath.dirname(p) == path
        )

    def _add(self, path: str, object_type: ObjectType, language=None, size=None):
        self.objects[path] = ObjectInfo(
            path=path,
            object_type=object_type,
            language=language,
            size=size,
            object_id=self._next_id,
        )
        self._next_id += 1

    def get_status(self, path):
        self.calls.append(("get_status", path))
        return self._status(path)

    def list(self, path):
        self.calls.append(("list", path))
        info = self._status(path)
        if info.object_type != ObjectType.DIRECTORY:
            return iter([info])
        return iter([self.objects[p] for p in self._children(path)])

    def mkdirs(self, path):
        self.calls.append(("mkdirs", path))
        parts = path.strip("/").split("/")
        for i in range(1, len(parts) + 1):
            p = "/" + "/".join(parts[:i])
            if p in self.objects:
                if self.objects[p].object_type != ObjectType.DIRECTORY:
                    raise ResourceAlreadyExists(f"{p} exists")
            else:
                self._add(p, ObjectType.DIRECTORY)

    def delete(self, path, recursive=False):
        self.calls.append(("delete", path, recursive))
        self._status(path)
        children = self._children(path)
        if children and not recursive:
            raise BadRequest(f"Folder ({path}) is not empty")
        for p in [p for p in self.objects if p == path or p.startswith(path + "/")]:
            del self.objects[p]
            self.data.pop(p, None)

    def download(self, path, format=None):
        self.calls.append(("download", path, format))
        info = self._status(path)
        if info.object_type == ObjectType.DIRECTORY:
            raise BadRequest(f"{path} is a directory")
        return BytesIO(self.data[path])

    def upload(self, path, content, *, format=None, language=None, overwrite=False):
        self.calls.append(("upload", path, format, language, overwrite))
        if hasattr(content, "read"):
            content = content.read()
        self._status(posixpath.dirname(path) or "/")
        if path in self.objects and not overwrite:
            raise ResourceAlreadyExists(f"{path} already exists")
        if format in (None, ImportFormat.RAW, ImportFormat.AUTO):
            self._add(path, ObjectType.FILE, size=len(content))
        else:
            self._add(path, ObjectType.NOTEBOOK, language=Language(language))
        self.data[path] = bytes(content)

    def calls_of(self, name: str) -> list[tuple]:
        return [c for c in self.calls if c[0] == name]


default_workspace_test_root = "/fsspec-databricks-test/local"
default_dbfs_test_root = "/fsspec-databricks-test/local"


def _test_root(variable: str, default: str) -> str:
    return "/" + os.getenv(variable, default).strip("/")


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


def fake_workspace_client() -> MagicMock:
    """A `WorkspaceClient` whose `workspace` is a `FakeWorkspaceAPI`."""
    client = MagicMock()
    client.workspace = FakeWorkspaceAPI()
    return client
