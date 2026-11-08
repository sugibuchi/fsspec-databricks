import inspect
import pickle
from random import randbytes
from unittest.mock import PropertyMock, patch

import fsspec
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from fsspec_alt_strategies.monitor import Monitor, monitoring

from fsspec_databricks import DatabricksFileSystem, use
from tests.utils import (
    init_dbfs_test_dir,
    init_volume_test_dir,
    init_workspace_test_dir,
    volume_download,
    workspace_download,
)


def volume_dbfs_url(path: str) -> str:
    return f"dbfs:/{path.lstrip('/')}"


def workspace_dbfs_url(path: str) -> str:
    return f"dbfs:/Workspace/{path.lstrip('/')}"


def test_use():
    use()

    fs_class = fsspec.get_filesystem_class("dbfs")
    assert fs_class is DatabricksFileSystem

    fs = fsspec.filesystem("dbfs")
    assert isinstance(fs, DatabricksFileSystem)


def test_dbx_fs_accepts_workspace_client_params():
    """`DatabricksFileSystem` accepts all parameters of `WorkspaceClient` in the installed SDK."""
    ws_params = set(inspect.signature(WorkspaceClient.__init__).parameters)
    fs_params = set(inspect.signature(DatabricksFileSystem.__init__).parameters)

    assert ws_params - fs_params == set()


def test_dbx_fs_config_params():
    with DatabricksFileSystem(
        host="https://example.cloud.databricks.com",
        group_id="group-1",
        workspace_id=1234567890,
        custom_headers={"X-Test": "1"},
    ) as fs:
        assert fs._config_params["host"] == "https://example.cloud.databricks.com"
        assert fs._config_params["group_id"] == "group-1"
        assert fs._config_params["workspace_id"] == 1234567890
        assert fs._config_params["custom_headers"] == {"X-Test": "1"}


def test_dbx_fs_local_legacy_dbfs(tmp_path):
    """In a workspace, legacy DBFS paths are under the local mount point, and paths returned are DBFS paths."""
    with DatabricksFileSystem() as fs:
        fs._running_in_workspace = True
        fs._legacy_dbfs_enabled = True
        fs.legacy_dbfs_local_root = tmp_path.as_posix()

        fs.makedirs("dbfs:/data")
        assert (tmp_path / "data").is_dir()

        with fs.open("dbfs:/data/a.txt", "wb") as f:
            f.write(b"hello")
        assert (tmp_path / "data" / "a.txt").read_bytes() == b"hello"

        assert fs.exists("dbfs:/data/a.txt")
        assert fs.ls("dbfs:/data", detail=False) == ["/data/a.txt"]
        assert [i["name"] for i in fs.ls("/data")] == ["/data/a.txt"]
        assert fs.info("/data/a.txt")["name"] == "/data/a.txt"
        assert fs.find("dbfs:/data") == ["/data/a.txt"]

        with fs.open(fs.ls("/data", detail=False)[0], "rb") as f:
            assert f.read() == b"hello"

        fs.cp_file("dbfs:/data/a.txt", "dbfs:/data/b.txt")
        assert (tmp_path / "data" / "b.txt").read_bytes() == b"hello"

        fs.rm("dbfs:/data/a.txt")
        assert not (tmp_path / "data" / "a.txt").exists()


def test_dbx_fs_local_root(tmp_path):
    """In a workspace, the root is listed without calling the Databricks API."""
    no_api = patch.object(
        DatabricksFileSystem,
        "client",
        new_callable=PropertyMock,
        side_effect=AssertionError("API called"),
    )

    # Without the legacy DBFS mount: the Volumes and Workspace roots
    with no_api, DatabricksFileSystem() as fs:
        fs._running_in_workspace = True
        fs.legacy_dbfs_local_root = (tmp_path / "missing").as_posix()

        assert not fs.legacy_dbfs_enabled
        assert fs.ls("/", detail=False) == ["/Volumes", "/Workspace"]
        assert fs.info("dbfs:/")["type"] == "directory"

    # With the legacy DBFS mount: its contents, as DBFS paths
    (tmp_path / "data").mkdir()
    with no_api, DatabricksFileSystem() as fs:
        fs._running_in_workspace = True
        fs.legacy_dbfs_local_root = tmp_path.as_posix()

        assert fs.legacy_dbfs_enabled
        assert fs.ls("/", detail=False) == ["/data"]


class _Calls(Monitor):
    """Records the calls whose operations it is told of."""

    def __init__(self):
        self.calls = []

    async def started(self, call, state, op):
        self.calls.append(call)


@pytest.fixture
def routed(client, workspace_test_root, dbfs_client, dbfs_test_root):
    """A `DatabricksFileSystem` outside a workspace, a Workspace directory of files, and an empty DBFS directory."""
    ws = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "dir": {
                "a.bin": (b"a", ImportFormat.RAW, None),
                "sub": {"b.bin": (b"bb", ImportFormat.RAW, None)},
            }
        },
    )
    dbfs = init_dbfs_test_dir(dbfs_client, dbfs_test_root)
    with DatabricksFileSystem(client=client) as fs:
        fs._running_in_workspace = False
        yield fs, f"/Workspace{ws}", dbfs


@pytest.mark.backend("workspace", "dbfs")
def test_dbx_fs_routes_recursive_operations(routed):
    """Recursive operations run in the backend of their path, with its strategies, which monitors see."""
    fs, ws, _ = routed
    monitor = _Calls()
    with monitoring(monitor):
        assert fs.find(f"dbfs:{ws}/dir") == [f"{ws}/dir/a.bin", f"{ws}/dir/sub/b.bin"]
        assert fs.glob(f"{ws}/dir/*.bin") == [f"{ws}/dir/a.bin"]
        assert fs.du(f"{ws}/dir") == 3
        fs.copy(f"{ws}/dir", f"{ws}/copy", recursive=True)

    # The backend's calls, with its paths, made through the router.
    backend = fs._workspace_fs
    assert [(c.fs, c.method, c.path) for c in monitor.calls] == [
        (backend, "find", f"{ws}/dir"),
        (backend, "glob", f"{ws}/dir/*.bin"),
        (backend, "du", f"{ws}/dir"),
        (backend, "copy", f"{ws}/dir"),
    ]
    assert all(c.via == (fs,) for c in monitor.calls)
    assert fs.find(f"{ws}/copy") == [f"{ws}/copy/a.bin", f"{ws}/copy/sub/b.bin"]
    assert [root for root, _, _ in fs.walk(f"{ws}/dir")] == [
        f"{ws}/dir",
        f"{ws}/dir/sub",
    ]


@pytest.mark.backend("workspace", "dbfs")
def test_dbx_fs_copies_between_backends(routed):
    """A copy between backends is the router's own, streaming each file through the backends' file objects."""
    fs, ws, dbfs = routed
    monitor = _Calls()
    with monitoring(monitor):
        fs.copy(f"{ws}/dir", f"{dbfs}/backup", recursive=True)
    assert [(c.fs, c.method, c.path) for c in monitor.calls] == [
        (fs, "copy", f"{ws}/dir")
    ]

    assert fs.find(f"{dbfs}/backup") == [
        f"{dbfs}/backup/a.bin",
        f"{dbfs}/backup/sub/b.bin",
    ]
    assert fs.cat_file(f"{dbfs}/backup/sub/b.bin") == b"bb"

    fs.mv(f"{dbfs}/backup/a.bin", f"{ws}/a.bin")
    assert not fs.exists(f"{dbfs}/backup/a.bin")
    assert fs.cat_file(f"{ws}/a.bin") == b"a"


@pytest.mark.backend("workspace", "dbfs")
def test_dbx_fs_root_without_legacy_dbfs(routed, monkeypatch):
    """Without legacy DBFS, the root holds the Volumes and Workspace roots, and cannot be changed."""
    fs, _, _ = routed

    async def refused(path, **kwargs):
        # As the DBFS API answers a workspace disabling legacy DBFS.
        raise PermissionError(path)

    monkeypatch.setattr(fs._legacy_fs, "_ls", refused)
    assert fs.ls("/", detail=False) == ["/Volumes", "/Workspace"]
    assert fs.info("dbfs:/")["type"] == "directory"
    with pytest.raises(PermissionError):
        fs.rm("/", recursive=True)


@pytest.mark.backend("workspace", "dbfs")
def test_dbx_fs_transaction_spans_backends(routed):
    """The router's transaction commits, or discards, the files opened on every backend."""
    fs, ws, dbfs = routed
    with fs.transaction:
        fs.pipe_file(f"{ws}/new.bin", b"w")
        with fs.open(f"{dbfs}/new.bin", "wb") as f:
            f.write(b"d")
        assert not fs.exists(f"{ws}/new.bin")
        assert not fs.exists(f"{dbfs}/new.bin")
    assert fs.cat_file(f"{ws}/new.bin") == b"w"
    assert fs.cat_file(f"{dbfs}/new.bin") == b"d"
    # The backends' own transactions hold nothing.
    assert not fs._workspace_fs.transaction.files
    assert not fs._legacy_fs.transaction.files

    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open(f"{ws}/lost.bin", "wb") as f:
            f.write(b"w")
        with fs.open(f"{dbfs}/lost.bin", "wb") as f:
            f.write(b"d")
        raise ValueError("dummy")
    assert not fs.exists(f"{ws}/lost.bin")
    assert fs.ls(dbfs, detail=False) == [f"{dbfs}/new.bin"]


def test_dbx_fs_init():
    with DatabricksFileSystem() as fs:
        assert fs.use_local_fs_in_workspace

    with DatabricksFileSystem(
        operation_concurrency=3,
        io_concurrency=4,
        use_local_fs_in_workspace=False,
    ) as fs:
        assert fs._volume_fs.operation_concurrency == 3
        assert fs._volume_fs.io_concurrency == 4

        assert not fs.use_local_fs_in_workspace


def test_dbx_fs_close():
    fs = DatabricksFileSystem()
    _ = fs._legacy_fs
    _ = fs._volume_fs
    _ = fs._workspace_fs
    _ = fs._local_fs

    fs.close()

    assert fs.closed

    with pytest.raises(RuntimeError):
        _ = fs._legacy_fs

    with pytest.raises(RuntimeError):
        _ = fs._volume_fs

    with pytest.raises(RuntimeError):
        _ = fs._workspace_fs

    with pytest.raises(RuntimeError):
        _ = fs._local_fs


def test_dbx_fs_serialization():
    fs = DatabricksFileSystem()
    _ = fs._legacy_fs
    _ = fs._volume_fs
    _ = fs._workspace_fs
    _ = fs._local_fs

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, DatabricksFileSystem)
    assert fs is not fs2
    assert fs._legacy_fs is not fs2._legacy_fs
    assert fs._volume_fs is not fs2._volume_fs
    assert fs._workspace_fs is not fs2._workspace_fs
    assert fs._local_fs is fs2._local_fs  # Local FS is cacheable


@pytest.mark.backend("volume", "workspace")
def test_dbx_fs_copy_file(
    client: WorkspaceClient, volume_test_root, workspace_test_root
):
    data_vol = randbytes(65536)
    data_ws = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={"file1.bin": data_vol},
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={"file2.bin": (data_ws, ImportFormat.RAW, None)},
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/file1.bin"
    dest = workspace_test_dir + "/file1.bin"
    fs.cp_file(volume_dbfs_url(src), workspace_dbfs_url(dest))
    assert workspace_download(client, dest) == data_vol

    src = workspace_test_dir + "/file2.bin"
    dest = volume_test_dir + "/file2.bin"

    fs.cp_file(workspace_dbfs_url(src), volume_dbfs_url(dest))
    assert volume_download(client, dest) == data_ws


@pytest.mark.backend("volume", "workspace")
def test_dbx_fs_copy(client: WorkspaceClient, volume_test_root, workspace_test_root):
    data_vol1 = randbytes(65536)
    data_vol2 = randbytes(65536)
    data_ws1 = randbytes(65536)
    data_ws2 = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={
            "dir_1": {
                "file1.bin": data_vol1,
                "file2.bin": data_vol2,
            }
        },
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "dir_2": {
                "file1.bin": (data_ws1, ImportFormat.RAW, None),
                "file2.bin": (data_ws2, ImportFormat.RAW, None),
            }
        },
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/dir_1"
    dest = workspace_test_dir + "/dir_1"
    fs.copy(volume_dbfs_url(src), workspace_dbfs_url(dest), recursive=True)
    assert workspace_download(client, dest + "/file1.bin") == data_vol1
    assert workspace_download(client, dest + "/file2.bin") == data_vol2

    src = workspace_test_dir + "/dir_2"
    dest = volume_test_dir + "/dir_2"

    fs.copy(workspace_dbfs_url(src), volume_dbfs_url(dest), recursive=True)
    assert volume_download(client, dest + "/file1.bin") == data_ws1
    assert volume_download(client, dest + "/file2.bin") == data_ws2


@pytest.mark.backend("volume", "workspace")
def test_dbx_fs_move(client: WorkspaceClient, volume_test_root, workspace_test_root):
    data_vol_1 = randbytes(65536)
    data_vol_3 = randbytes(65536)
    data_fs_4 = randbytes(65536)
    data_fs_6 = randbytes(65536)

    volume_test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        items={
            "file1.bin": data_vol_1,
            "dir_2": {
                "file3.bin": data_vol_3,
            },
        },
    )

    workspace_test_dir = init_workspace_test_dir(
        client,
        workspace_test_root,
        items={
            "file4.bin": (data_fs_4, ImportFormat.RAW, None),
            "dir_5": {
                "file6.bin": (data_fs_6, ImportFormat.RAW, None),
            },
        },
    )

    fs = DatabricksFileSystem(client=client)

    src = volume_test_dir + "/file1.bin"
    dest = workspace_test_dir + "/file1.bin"

    fs.move(volume_dbfs_url(src), workspace_dbfs_url(dest))
    assert not fs.exists(volume_dbfs_url(src))
    assert workspace_download(client, dest) == data_vol_1

    src = volume_test_dir + "/dir_2"
    dest = workspace_test_dir + "/dir_2"

    fs.move(volume_dbfs_url(src), workspace_dbfs_url(dest), recursive=True)
    assert not fs.exists(volume_dbfs_url(src))
    assert fs.exists(workspace_dbfs_url(dest))
    assert workspace_download(client, dest + "/file3.bin") == data_vol_3

    src = workspace_test_dir + "/file4.bin"
    dest = volume_test_dir + "/file4.bin"

    fs.move(workspace_dbfs_url(src), volume_dbfs_url(dest))
    assert not fs.exists(workspace_dbfs_url(src))
    assert volume_download(client, dest) == data_fs_4

    src = workspace_test_dir + "/dir_5"
    dest = volume_test_dir + "/dir_5"

    fs.move(workspace_dbfs_url(src), volume_dbfs_url(dest), recursive=True)
    assert not fs.exists(workspace_dbfs_url(src))
    assert fs.exists(volume_dbfs_url(dest))
    assert volume_download(client, dest + "/file6.bin") == data_fs_6
