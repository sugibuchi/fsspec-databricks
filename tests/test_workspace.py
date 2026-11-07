"""Offline tests of `WorkspaceFileSystem`, on the fake Workspace API of `tests/utils.py`."""

import errno

import pytest
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from fsspec.asyn import sync
from fsspec_alt_strategies_common.monitor import Monitor, monitoring

from fsspec_databricks.workspace import WorkspaceFileSystem

from .utils import fake_workspace_client


class Calls(Monitor):
    """Records the calls whose operations it is told of."""

    def __init__(self):
        self.calls = []

    async def started(self, call, state, op):
        self.calls.append(call)


@pytest.fixture
def client():
    client = fake_workspace_client()
    ws = client.workspace
    ws.mkdirs("/t/dir/sub")
    ws.mkdirs("/t/empty")
    ws.upload("/t/data.bin", b"data", format=ImportFormat.RAW)
    ws.upload("/t/dir/a.bin", b"a", format=ImportFormat.RAW)
    ws.upload("/t/dir/sub/b.bin", b"b", format=ImportFormat.RAW)
    ws.upload(
        "/t/dir/nb", b"# nb", format=ImportFormat.SOURCE, language=Language.PYTHON
    )
    ws.calls.clear()
    return client


@pytest.fixture
def fs(client):
    with WorkspaceFileSystem(client=client) as fs:
        yield fs


def test_workspace_info_and_ls(fs):
    assert fs.info("/")["type"] == "directory"
    assert fs.ls("/", detail=False) == ["/Workspace"]

    info = fs.info("dbfs:/Workspace/t/dir/nb")
    assert info["name"] == "/Workspace/t/dir/nb"
    assert info["type"] == "file"
    assert info["object_type"] == "NOTEBOOK"
    assert info["language"] == "PYTHON"

    assert fs.ls("/Workspace/t/dir", detail=False) == [
        "/Workspace/t/dir/a.bin",
        "/Workspace/t/dir/nb",
        "/Workspace/t/dir/sub",
    ]
    with pytest.raises(FileNotFoundError):
        fs.info("/Workspace/t/missing")
    with pytest.raises(FileNotFoundError):
        fs.info("/Volumes/c/s/v")


def test_workspace_find_runs_tree_strategy(fs, client):
    """`find` is the tree strategy: one listing per folder, seen by monitors."""
    monitor = Calls()
    with monitoring(monitor):
        found = fs.find("/Workspace/t/dir")

    assert found == [
        "/Workspace/t/dir/a.bin",
        "/Workspace/t/dir/nb",
        "/Workspace/t/dir/sub/b.bin",
    ]
    assert [(c.fs, c.method) for c in monitor.calls] == [(fs, "find")]
    assert sorted(c[1] for c in client.workspace.calls_of("list")) == [
        "/t/dir",
        "/t/dir/sub",
    ]


def test_workspace_rm_recursive_is_native(fs, client):
    """A recursive `rm` without patterns is the API's own recursive delete: one request."""
    fs.rm("/Workspace/t/dir", recursive=True)

    assert client.workspace.calls_of("delete") == [("delete", "/t/dir", True)]
    assert not fs.exists("/Workspace/t/dir")

    with pytest.raises(FileNotFoundError):
        fs.rm("/Workspace/t/dir", recursive=True)


def test_workspace_rm_glob_runs_tree_strategy(fs, client):
    fs.rm("/Workspace/t/dir/*.bin", recursive=True)

    assert not fs.exists("/Workspace/t/dir/a.bin")
    assert fs.exists("/Workspace/t/dir/nb")
    assert ("delete", "/t/dir/a.bin", False) in client.workspace.calls


def test_workspace_rm_refuses_directories(fs, client):
    with pytest.raises(IsADirectoryError):
        fs.rm("/Workspace/t/empty")
    with pytest.raises(IsADirectoryError):
        fs.rm_file("/Workspace/t/empty")

    assert client.workspace.calls_of("delete") == []


def test_workspace_rmdir(fs, client):
    with pytest.raises(OSError) as e:
        fs.rmdir("/Workspace/t/dir")
    assert e.value.errno == errno.ENOTEMPTY

    with pytest.raises(NotADirectoryError):
        fs.rmdir("/Workspace/t/data.bin")

    fs.rmdir("/Workspace/t/empty")
    assert not fs.exists("/Workspace/t/empty")


def test_workspace_mkdir(fs):
    fs.mkdir("/Workspace/t/new")
    assert fs.isdir("/Workspace/t/new")

    with pytest.raises(FileExistsError):
        fs.mkdir("/Workspace/t/new")
    with pytest.raises(FileNotFoundError):
        fs.mkdir("/Workspace/t/x/y", create_parents=False)
    with pytest.raises(NotADirectoryError):
        fs.makedirs("/Workspace/t/data.bin", exist_ok=True)

    fs.makedirs("/Workspace/t/x/y")
    fs.makedirs("/Workspace/t/x/y", exist_ok=True)
    assert fs.isdir("/Workspace/t/x/y")


def test_workspace_copy_keeps_notebooks(fs, client):
    fs.copy("/Workspace/t/dir", "/Workspace/t/copy", recursive=True)

    assert fs.find("/Workspace/t/copy", withdirs=True) == [
        "/Workspace/t/copy",
        "/Workspace/t/copy/a.bin",
        "/Workspace/t/copy/nb",
        "/Workspace/t/copy/sub",
        "/Workspace/t/copy/sub/b.bin",
    ]
    nb = fs.info("/Workspace/t/copy/nb")
    assert (nb["object_type"], nb["language"]) == ("NOTEBOOK", "PYTHON")
    assert ("download", "/t/dir/nb", ExportFormat.SOURCE) in client.workspace.calls


def test_workspace_open(fs, client):
    with fs.open("/Workspace/t/data.bin", "rb") as f:
        assert f.read() == b"data"

    with fs.open("/Workspace/t/data.bin", "ab") as f:
        f.write(b"+more")
    assert client.workspace.data["/t/data.bin"] == b"data+more"

    with fs.open("/Workspace/t/text.txt", "wt") as f:
        f.write("a\nb")
    assert fs.cat_file("/Workspace/t/text.txt") == b"a\nb"

    with pytest.raises(FileExistsError):
        fs.open("/Workspace/t/data.bin", "xb")
    with fs.open("/Workspace/t/new.bin", "xb") as f:
        f.write(b"new")
    assert client.workspace.calls_of("upload")[-1] == (
        "upload",
        "/t/new.bin",
        ImportFormat.RAW,
        None,
        False,
    )

    with pytest.raises(IsADirectoryError):
        fs.open("/Workspace/t/dir", "rb")
    with pytest.raises(FileNotFoundError):
        fs.open("/Workspace/t/missing", "rb")


def test_workspace_open_notebook(fs, client):
    with pytest.raises(ValueError, match="language must be specified"):
        fs.open("/Workspace/t/new_nb", "wb", notebook_format="SOURCE")

    with fs.open(
        "/Workspace/t/new_nb", "wb", notebook_format="SOURCE", language="scala"
    ) as f:
        f.write(b"// nb")
    info = fs.info("/Workspace/t/new_nb")
    assert (info["object_type"], info["language"]) == ("NOTEBOOK", "SCALA")

    # An existing notebook keeps its language.
    with fs.open("/Workspace/t/dir/nb", "wb") as f:
        f.write(b"# changed")
    assert client.workspace.calls_of("upload")[-1] == (
        "upload",
        "/t/dir/nb",
        ImportFormat.SOURCE,
        Language.PYTHON,
        True,
    )

    with pytest.raises(ValueError, match="not applicable"):
        fs.open("/Workspace/t/data.bin", "rb", notebook_format="JUPYTER")


def test_workspace_file_size_limit(fs, client):
    with pytest.raises(OSError) as e, fs.open("/Workspace/t/big.bin", "wb") as f:
        f.write(b"x" * (10 * 1024 * 1024 + 1))
    assert e.value.errno == errno.EFBIG
    assert "/t/big.bin" not in client.workspace.data


def test_workspace_open_async(fs):
    async def read():
        f = await fs.open_async("/Workspace/t/data.bin")
        async with f:
            assert f.size == 4
            f.seek(2)
            return await f.read()

    assert sync(fs.loop, read) == b"ta"
