"""Tests of `DBFS`, through the SDK over HTTP: on the dummy server, and on the live workspace when it enables legacy
DBFS."""

import errno
from random import randbytes

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from fsspec.asyn import sync

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.dbfs import DBFS
from tests.utils import bytes_sig, dbfs_download, init_dbfs_test_dir

pytestmark = pytest.mark.backend("dbfs")

fs_classes = pytest.mark.parametrize("fs_class", [DBFS, DatabricksFileSystem])


def exists(client: WorkspaceClient, path: str) -> bool:
    try:
        client.dbfs.get_status(path)
        return True
    except ResourceDoesNotExist:
        return False


@fs_classes
def test_dbfs_info_and_ls(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    data = randbytes(12345)
    test_dir = init_dbfs_test_dir(
        client, dbfs_test_root, fs_class, {"1_data.bin": data, "2_directory": {}}
    )
    fs = fs_class(client=client)

    info = fs.info(f"dbfs:{test_dir}/1_data.bin")
    assert info["name"] == f"{test_dir}/1_data.bin"
    assert (info["size"], info["type"]) == (len(data), "file")
    assert fs.info(f"{test_dir}/2_directory")["type"] == "directory"
    with pytest.raises(FileNotFoundError):
        fs.info(f"{test_dir}/nonexistent")

    assert fs.ls(f"dbfs:{test_dir}", detail=False) == [
        f"{test_dir}/1_data.bin",
        f"{test_dir}/2_directory",
    ]
    assert fs.ls(f"{test_dir}/2_directory", detail=False) == []
    with pytest.raises(FileNotFoundError):
        fs.ls(f"{test_dir}/nonexistent")

    assert fs.exists(f"{test_dir}/1_data.bin")
    assert not fs.exists(f"{test_dir}/nonexistent")


def test_dbfs_info_root(dbfs_client):
    assert DBFS(client=dbfs_client).info("/")["name"] == "/"


@fs_classes
def test_dbfs_mkdir_and_makedirs(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    test_dir = init_dbfs_test_dir(client, dbfs_test_root, fs_class, {"data.bin": b"x"})
    fs = fs_class(client=client)

    fs.mkdir(f"{test_dir}/dir")
    assert client.dbfs.get_status(f"{test_dir}/dir").is_dir
    with pytest.raises(FileExistsError):
        fs.mkdir(f"{test_dir}/dir")
    with pytest.raises(FileNotFoundError):
        fs.mkdir(f"{test_dir}/parent/dir", create_parents=False)

    fs.makedirs(f"{test_dir}/parent/dir")
    assert client.dbfs.get_status(f"{test_dir}/parent/dir").is_dir
    with pytest.raises(FileExistsError):
        fs.makedirs(f"{test_dir}/parent/dir", exist_ok=False)
    fs.makedirs(f"{test_dir}/parent/dir", exist_ok=True)


@fs_classes
def test_dbfs_rm(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    test_dir = init_dbfs_test_dir(
        client,
        dbfs_test_root,
        fs_class,
        {"1_data.bin": b"1", "2_directory": {"3_data.bin": b"3", "4_sub": {}}},
    )
    fs = fs_class(client=client)

    fs.rm(f"{test_dir}/1_data.bin")
    assert not exists(client, f"{test_dir}/1_data.bin")

    with pytest.raises(IsADirectoryError):
        fs.rm(f"{test_dir}/2_directory", recursive=False)
    fs.rm(f"{test_dir}/2_directory", recursive=True)
    assert not exists(client, f"{test_dir}/2_directory")

    with pytest.raises(FileNotFoundError):
        fs.rm(f"{test_dir}/nonexistent")
    with pytest.raises(FileNotFoundError):
        fs.rm(f"{test_dir}/2_directory", recursive=True)


def test_dbfs_rm_recursive_is_native(dbfs_client, dbfs_test_root, dummy):
    """A recursive `rm` is the API's own recursive delete: one request."""
    test_dir = init_dbfs_test_dir(
        dbfs_client, dbfs_test_root, items={"dir": {"a.bin": b"a", "sub": {}}}
    )
    dummy.requests.clear()

    DBFS(client=dbfs_client).rm(f"{test_dir}/dir", recursive=True)

    deletes = dummy.requests_to("/api/2.0/dbfs/delete")
    assert [r.params for r in deletes] == [
        {"path": f"{test_dir}/dir", "recursive": True}
    ]


@fs_classes
def test_dbfs_rmdir(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    test_dir = init_dbfs_test_dir(
        client,
        dbfs_test_root,
        fs_class,
        {"1_directory": {}, "2_data.bin": b"2", "3_full": {"4_data.bin": b"4"}},
    )
    fs = fs_class(client=client)

    fs.rmdir(f"{test_dir}/1_directory")
    assert not exists(client, f"{test_dir}/1_directory")
    with pytest.raises(FileNotFoundError):
        fs.rmdir(f"{test_dir}/nonexistent")
    with pytest.raises(NotADirectoryError):
        fs.rmdir(f"{test_dir}/2_data.bin")
    with pytest.raises(OSError) as e:
        fs.rmdir(f"{test_dir}/3_full")
    assert e.value.errno == errno.ENOTEMPTY
    assert exists(client, f"{test_dir}/3_full/4_data.bin")


def test_dbfs_rmdir_never_recursive(dbfs_client, dbfs_test_root, dummy):
    """rmdir deletes only an empty directory, with a non-recursive delete, and never asks for a full one."""
    test_dir = init_dbfs_test_dir(
        dbfs_client, dbfs_test_root, items={"full": {"a.bin": b"a"}, "empty": {}}
    )
    fs = DBFS(client=dbfs_client)
    dummy.requests.clear()

    with pytest.raises(OSError):
        fs.rmdir(f"{test_dir}/full")
    assert dummy.requests_to("/api/2.0/dbfs/delete") == []

    fs.rmdir(f"{test_dir}/empty")
    deletes = dummy.requests_to("/api/2.0/dbfs/delete")
    assert [r.params for r in deletes] == [
        {"path": f"{test_dir}/empty", "recursive": False}
    ]


def test_dbfs_rmdir_refused_by_api(dbfs_client, dbfs_test_root, dummy, monkeypatch):
    """A directory filled after it was listed is refused by the API's non-recursive delete."""
    test_dir = init_dbfs_test_dir(
        dbfs_client, dbfs_test_root, items={"dir": {"a.bin": b"a"}}
    )
    fs = DBFS(client=dbfs_client)
    # The listing misses the file, as one written just after it would be.
    listed = dbfs_client.dbfs.list
    monkeypatch.setattr(
        fs.client.dbfs,
        "list",
        lambda path, **kw: iter([]) if path == f"{test_dir}/dir" else listed(path),
    )

    with pytest.raises(OSError) as e:
        fs.rmdir(f"{test_dir}/dir")
    assert e.value.errno == errno.ENOTEMPTY
    assert exists(dbfs_client, f"{test_dir}/dir/a.bin")


@fs_classes
def test_dbfs_mv(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    data = randbytes(12345)
    test_dir = init_dbfs_test_dir(
        client,
        dbfs_test_root,
        fs_class,
        {
            "1_data.bin": data,
            "2_directory": {"3_data.bin": b"3", "4_sub": {"5.bin": b"5"}},
        },
    )
    fs = fs_class(client=client)

    fs.mv(f"{test_dir}/1_data.bin", f"{test_dir}/1_moved.bin")
    assert not exists(client, f"{test_dir}/1_data.bin")
    assert bytes_sig(dbfs_download(client, f"{test_dir}/1_moved.bin")) == bytes_sig(
        data
    )

    fs.mv(f"{test_dir}/2_directory", f"{test_dir}/2_moved", recursive=True)
    assert not exists(client, f"{test_dir}/2_directory")
    assert fs.find(f"{test_dir}/2_moved") == [
        f"{test_dir}/2_moved/3_data.bin",
        f"{test_dir}/2_moved/4_sub/5.bin",
    ]


def test_dbfs_mv_is_native(dbfs_client, dbfs_test_root, dummy):
    """A file moves with one move, and a folder whole, as the profile declares."""
    test_dir = init_dbfs_test_dir(
        dbfs_client,
        dbfs_test_root,
        items={"data.bin": b"d", "dir": {"a.bin": b"a", "sub": {"b.bin": b"b"}}},
    )
    fs = DBFS(client=dbfs_client)
    dummy.requests.clear()

    fs.mv(f"{test_dir}/data.bin", f"{test_dir}/moved.bin")
    fs.mv(f"{test_dir}/dir", f"{test_dir}/moved", recursive=True)

    moves = dummy.requests_to("/api/2.0/dbfs/move")
    assert [r.params for r in moves] == [
        {
            "source_path": f"{test_dir}/data.bin",
            "destination_path": f"{test_dir}/moved.bin",
        },
        {"source_path": f"{test_dir}/dir", "destination_path": f"{test_dir}/moved"},
    ]
    assert fs.find(f"{test_dir}/moved") == [
        f"{test_dir}/moved/a.bin",
        f"{test_dir}/moved/sub/b.bin",
    ]


@fs_classes
def test_dbfs_cp(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    data = randbytes(12345)
    test_dir = init_dbfs_test_dir(
        client,
        dbfs_test_root,
        fs_class,
        {"1_data.bin": data, "2_directory": {"3_data.bin": b"3", "4_empty": {}}},
    )
    fs = fs_class(client=client)

    fs.cp_file(f"{test_dir}/1_data.bin", f"{test_dir}/1_copied.bin")
    assert bytes_sig(dbfs_download(client, f"{test_dir}/1_copied.bin")) == bytes_sig(
        data
    )
    with pytest.raises(FileNotFoundError):
        fs.cp_file(f"{test_dir}/nonexistent", f"{test_dir}/nonexistent_copy")

    fs.copy(f"{test_dir}/2_directory", f"{test_dir}/2_copied", recursive=True)
    assert dbfs_download(client, f"{test_dir}/2_copied/3_data.bin") == b"3"
    assert client.dbfs.get_status(f"{test_dir}/2_copied/4_empty").is_dir


@fs_classes
def test_dbfs_open(dbfs_client, fs_class, dbfs_test_root):
    client = dbfs_client
    # Over 1 MB: the SDK's files read and write it in blocks of the API's limit.
    data = randbytes(2_500_000)
    test_dir = init_dbfs_test_dir(
        client, dbfs_test_root, fs_class, {"1_data.bin": data, "2_directory": {}}
    )
    fs = fs_class(client=client)

    with fs.open(f"{test_dir}/1_data.bin", "rb") as f:
        assert bytes_sig(f.read()) == bytes_sig(data)
    # A read over the API's 1 MB limit, as copyfileobj makes: several requests.
    with fs.open(f"{test_dir}/1_data.bin", "rb") as f:
        assert f.read(2_000_000) == data[:2_000_000]
        assert bytes_sig(f.read(2_000_000)) == bytes_sig(data[2_000_000:])
    assert bytes_sig(fs.cat_file(f"{test_dir}/1_data.bin")) == bytes_sig(data)
    with pytest.raises(FileNotFoundError):
        fs.open(f"{test_dir}/nonexistent", "rb")
    with pytest.raises(IsADirectoryError):
        fs.open(f"{test_dir}/2_directory", "rb")

    with fs.open(f"{test_dir}/3_new.bin", "wb") as f:
        f.write(data)
    assert bytes_sig(dbfs_download(client, f"{test_dir}/3_new.bin")) == bytes_sig(data)

    with fs.open(f"{test_dir}/4_text.txt", "w") as f:
        f.write("text\n")
    assert dbfs_download(client, f"{test_dir}/4_text.txt") == b"text\n"

    with pytest.raises(FileExistsError):
        fs.open(f"{test_dir}/3_new.bin", "xb")
    with pytest.raises(IsADirectoryError):
        fs.open(f"{test_dir}/2_directory", "wb")
    with pytest.raises(OSError):
        fs.open(f"{test_dir}/3_new.bin", "ab")


def test_dbfs_open_async(dbfs_client, dbfs_test_root):
    test_dir = init_dbfs_test_dir(dbfs_client, dbfs_test_root, items={"a.bin": b"new"})
    fs = DBFS(client=dbfs_client)

    async def read():
        f = await fs.open_async(f"{test_dir}/a.bin")
        async with f:
            assert (f.mode, f.size) == ("rb", 3)
            return await f.read()

    assert sync(fs.loop, read) == b"new"


@fs_classes
def test_dbfs_transaction(dbfs_client, fs_class, dbfs_test_root):
    """A file written in a transaction goes to a temporary path, moved into place on commit."""
    client = dbfs_client
    test_dir = init_dbfs_test_dir(
        client, dbfs_test_root, fs_class, {"1_data.bin": b"old"}
    )
    fs = fs_class(client=client)
    data = randbytes(54321)

    with fs.transaction:
        with fs.open(f"{test_dir}/1_data.bin", "wb") as f:
            f.write(data)
        assert dbfs_download(client, f"{test_dir}/1_data.bin") == b"old"
    assert bytes_sig(dbfs_download(client, f"{test_dir}/1_data.bin")) == bytes_sig(data)

    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open(f"{test_dir}/2_data.bin", "wb") as f:
            f.write(data)
        raise ValueError("dummy")
    assert not exists(client, f"{test_dir}/2_data.bin")

    # Exclusive creation fails on commit: the API's move refuses the file there.
    with pytest.raises(FileExistsError), fs.transaction:
        with fs.open(f"{test_dir}/1_data.bin", "xb") as f:
            f.write(b"x")
    assert bytes_sig(dbfs_download(client, f"{test_dir}/1_data.bin")) == bytes_sig(data)
    # No temporary file is left.
    assert [i.path for i in client.dbfs.list(test_dir)] == [f"{test_dir}/1_data.bin"]


_setting = "/api/2.1/settings/disable_legacy_dbfs"


def test_dbfs_router_without_legacy_dbfs(dummy_client, dummy):
    """A workspace disabling legacy DBFS refuses DBFS paths by the API's 403, with no setting asked."""
    dummy.legacy_dbfs_disabled = True
    dummy.requests.clear()
    fs = DatabricksFileSystem(client=dummy_client)
    with pytest.raises(PermissionError):
        fs.info("dbfs:/tmp")
    with pytest.raises(PermissionError):
        fs.pipe_file("dbfs:/tmp/a.bin", b"a")
    # The root falls back to the roots of Volumes and Workspace.
    assert sorted(fs.ls("/", detail=False)) == ["/Volumes", "/Workspace"]
    with pytest.raises(PermissionError):
        DBFS(client=dummy_client).info("/tmp")
    assert dummy.requests_to(_setting) == []


def test_dbfs_router_root_with_legacy_dbfs(dummy_client, dummy):
    """The root lists the DBFS root, with no setting asked."""
    dummy.requests.clear()
    fs = DatabricksFileSystem(client=dummy_client)
    fs.ls("/", detail=False)
    assert len(dummy.requests_to("/api/2.0/dbfs/list")) == 1
    assert dummy.requests_to(_setting) == []


def test_dbfs_legacy_dbfs_enabled_asks_once(dummy_client, dummy):
    """The property asks the setting once per instance, and a new instance asks again."""
    dummy.legacy_dbfs_disabled = True
    dummy.requests.clear()
    fs = DatabricksFileSystem(client=dummy_client)
    assert not fs.legacy_dbfs_enabled
    assert not fs.legacy_dbfs_enabled
    assert len(dummy.requests_to(_setting)) == 1
    assert not DBFS(client=dummy_client).legacy_dbfs_enabled
    assert len(dummy.requests_to(_setting)) == 2


def test_dbfs_setting_not_asked_for_other_backends(dummy_client, dummy):
    """Volume and Workspace paths never ask the legacy DBFS setting."""
    fs = DatabricksFileSystem(client=dummy_client)
    dummy.requests.clear()
    for path in ["/Volumes/c/s/v/missing", "/Workspace/missing"]:
        with pytest.raises(FileNotFoundError):
            fs.info(path)
    assert dummy.requests_to(_setting) == []


### The adaptive files' requests, on the dummy server, which refuses over 1 MB per read, put and add-block

_MiB = 1024 * 1024


def _dbfs_requests(dummy, endpoint):
    return [r.params for r in dummy.requests_to(f"/api/2.0/dbfs/{endpoint}")]


def test_dbfs_open_requests(dbfs_client, dbfs_test_root, dummy):
    """A small file is one put; a large one a stream of add-blocks, in order; reads are ranges."""
    test_dir = init_dbfs_test_dir(dbfs_client, dbfs_test_root)
    fs = DBFS(client=dbfs_client)
    dummy.requests.clear()

    with fs.open(f"{test_dir}/small.bin", "wb") as f:
        f.write(b"small")
    assert [p["path"] for p in _dbfs_requests(dummy, "put")] == [
        f"{test_dir}/small.bin"
    ]
    assert _dbfs_requests(dummy, "create") == []

    dummy.requests.clear()
    data = randbytes(3 * _MiB + 12345)
    with fs.open(f"{test_dir}/large.bin", "wb") as f:
        for i in range(0, len(data), 300_000):
            f.write(data[i : i + 300_000])
    assert dbfs_download(dbfs_client, f"{test_dir}/large.bin") == data
    assert _dbfs_requests(dummy, "create") == [
        {"path": f"{test_dir}/large.bin", "overwrite": True}
    ]
    assert len(_dbfs_requests(dummy, "add-block")) >= 4
    assert len(_dbfs_requests(dummy, "close")) == 1

    dummy.requests.clear()
    with fs.open(f"{test_dir}/large.bin", "rb") as f:
        assert f.seekable()
        f.seek(2 * _MiB + 7)
        assert f.read(100) == data[2 * _MiB + 7 : 2 * _MiB + 107]
        assert f.tell() == 2 * _MiB + 107
        f.seek(0)
        assert f.read() == data
    reads = _dbfs_requests(dummy, "read")
    assert max(int(p["length"]) for p in reads) <= _MiB


def test_dbfs_open_exclusive(dbfs_client, dbfs_test_root, dummy):
    test_dir = init_dbfs_test_dir(dbfs_client, dbfs_test_root)
    fs = DBFS(client=dbfs_client)
    dummy.requests.clear()

    with fs.open(f"{test_dir}/x.bin", "xb") as f:
        f.write(randbytes(2 * _MiB))
    assert _dbfs_requests(dummy, "create") == [
        {"path": f"{test_dir}/x.bin", "overwrite": False}
    ]
    with pytest.raises(FileExistsError):
        fs.open(f"{test_dir}/x.bin", "xb")


def test_dbfs_open_aborts(dbfs_client, dbfs_test_root, dummy):
    """A stream cannot be aborted: the file it replaced is deleted, and a small file never sent."""
    test_dir = init_dbfs_test_dir(
        dbfs_client, dbfs_test_root, items={"data.bin": b"data"}
    )
    fs = DBFS(client=dbfs_client)

    with pytest.raises(ValueError, match="dummy"):
        with fs.open(f"{test_dir}/data.bin", "wb") as f:
            f.write(b"lost")
            raise ValueError("dummy")
    assert dbfs_download(dbfs_client, f"{test_dir}/data.bin") == b"data"

    with pytest.raises(ValueError, match="dummy"):
        with fs.open(f"{test_dir}/data.bin", "wb") as f:
            f.write(randbytes(3 * _MiB))
            raise ValueError("dummy")
    assert not fs.exists(f"{test_dir}/data.bin")
    assert dummy.dbfs_handles == {}

    dummy.dbfs_add_block_errors = 10
    with pytest.raises(OSError):
        with fs.open(f"{test_dir}/failed.bin", "wb") as f:
            f.write(randbytes(3 * _MiB))
    assert not fs.exists(f"{test_dir}/failed.bin")
    assert dummy.dbfs_handles == {}
