"""Tests of `DBFS`, through the SDK over HTTP: on the dummy server offline, and on the real workspace when it
enables legacy DBFS."""

import errno
import inspect
from random import randbytes

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.dbfs import DBFS
from tests.utils import bytes_sig, legacy_dbfs_disabled, prepare_dbfs_test_root

fs_classes = pytest.mark.parametrize("fs_class", [DBFS, DatabricksFileSystem])


@pytest.fixture(scope="module", params=["dummy", "live"])
def client(request) -> WorkspaceClient:
    """The dummy server's client, offline, then the real workspace's, when it enables legacy DBFS."""
    client = request.getfixturevalue(f"{request.param}_client")
    if request.param == "live" and legacy_dbfs_disabled(client):
        pytest.skip("The workspace disables legacy DBFS")
    return client


@pytest.fixture(scope="module")
def dbfs_test_root(client: WorkspaceClient) -> str:
    return prepare_dbfs_test_root(client)


def init_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type,
    items: dict[str, dict | bytes] | None = None,
) -> str:
    """Create an empty test directory, named after the test, with the given files and directories."""
    test_dir = f"{test_root}/{inspect.stack()[1].function}_{fs_class.__name__}"
    client.dbfs.delete(test_dir, recursive=True)
    client.dbfs.mkdirs(test_dir)

    def init(path: str, _items: dict[str, dict | bytes]):
        for name, item in _items.items():
            if isinstance(item, dict):
                client.dbfs.mkdirs(f"{path}/{name}")
                init(f"{path}/{name}", item)
            else:
                with client.dbfs.open(f"{path}/{name}", write=True) as f:
                    f.write(item)

    init(test_dir, items or {})
    return test_dir


def download(client: WorkspaceClient, path: str) -> bytes:
    with client.dbfs.download(path) as f:
        return f.read()


def exists(client: WorkspaceClient, path: str) -> bool:
    try:
        client.dbfs.get_status(path)
        return True
    except ResourceDoesNotExist:
        return False


@fs_classes
def test_dbfs_fs_info_and_ls(client, fs_class, dbfs_test_root):
    data = randbytes(12345)
    test_dir = init_test_dir(
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


@fs_classes
def test_dbfs_fs_mkdir_and_makedirs(client, fs_class, dbfs_test_root):
    test_dir = init_test_dir(client, dbfs_test_root, fs_class, {"data.bin": b"x"})
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
def test_dbfs_fs_rm(client, fs_class, dbfs_test_root):
    test_dir = init_test_dir(
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


@fs_classes
def test_dbfs_fs_rmdir(client, fs_class, dbfs_test_root):
    test_dir = init_test_dir(
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


@fs_classes
def test_dbfs_fs_mv(client, fs_class, dbfs_test_root):
    data = randbytes(12345)
    test_dir = init_test_dir(
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
    assert bytes_sig(download(client, f"{test_dir}/1_moved.bin")) == bytes_sig(data)

    fs.mv(f"{test_dir}/2_directory", f"{test_dir}/2_moved", recursive=True)
    assert not exists(client, f"{test_dir}/2_directory")
    assert fs.find(f"{test_dir}/2_moved") == [
        f"{test_dir}/2_moved/3_data.bin",
        f"{test_dir}/2_moved/4_sub/5.bin",
    ]


@fs_classes
def test_dbfs_fs_cp(client, fs_class, dbfs_test_root):
    data = randbytes(12345)
    test_dir = init_test_dir(
        client,
        dbfs_test_root,
        fs_class,
        {"1_data.bin": data, "2_directory": {"3_data.bin": b"3", "4_empty": {}}},
    )
    fs = fs_class(client=client)

    fs.cp_file(f"{test_dir}/1_data.bin", f"{test_dir}/1_copied.bin")
    assert bytes_sig(download(client, f"{test_dir}/1_copied.bin")) == bytes_sig(data)
    with pytest.raises(FileNotFoundError):
        fs.cp_file(f"{test_dir}/nonexistent", f"{test_dir}/nonexistent_copy")

    fs.copy(f"{test_dir}/2_directory", f"{test_dir}/2_copied", recursive=True)
    assert download(client, f"{test_dir}/2_copied/3_data.bin") == b"3"
    assert client.dbfs.get_status(f"{test_dir}/2_copied/4_empty").is_dir


@fs_classes
def test_dbfs_fs_open(client, fs_class, dbfs_test_root):
    # Over 1 MB: the SDK's files read and write it in blocks of the API's limit.
    data = randbytes(2_500_000)
    test_dir = init_test_dir(
        client, dbfs_test_root, fs_class, {"1_data.bin": data, "2_directory": {}}
    )
    fs = fs_class(client=client)

    with fs.open(f"{test_dir}/1_data.bin", "rb") as f:
        assert bytes_sig(f.read()) == bytes_sig(data)
    assert bytes_sig(fs.cat_file(f"{test_dir}/1_data.bin")) == bytes_sig(data)
    with pytest.raises(FileNotFoundError):
        fs.open(f"{test_dir}/nonexistent", "rb")
    with pytest.raises(IsADirectoryError):
        fs.open(f"{test_dir}/2_directory", "rb")

    with fs.open(f"{test_dir}/3_new.bin", "wb") as f:
        f.write(data)
    assert bytes_sig(download(client, f"{test_dir}/3_new.bin")) == bytes_sig(data)

    with fs.open(f"{test_dir}/4_text.txt", "w") as f:
        f.write("text\n")
    assert download(client, f"{test_dir}/4_text.txt") == b"text\n"

    with pytest.raises(FileExistsError):
        fs.open(f"{test_dir}/3_new.bin", "xb")
    with pytest.raises(IsADirectoryError):
        fs.open(f"{test_dir}/2_directory", "wb")


@fs_classes
def test_dbfs_fs_transaction(client, fs_class, dbfs_test_root):
    test_dir = init_test_dir(client, dbfs_test_root, fs_class, {"1_data.bin": b"old"})
    fs = fs_class(client=client)
    data = randbytes(54321)

    with fs.transaction:
        with fs.open(f"{test_dir}/1_data.bin", "wb") as f:
            f.write(data)
        assert download(client, f"{test_dir}/1_data.bin") == b"old"
    assert bytes_sig(download(client, f"{test_dir}/1_data.bin")) == bytes_sig(data)

    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open(f"{test_dir}/2_data.bin", "wb") as f:
            f.write(data)
        raise ValueError("dummy")
    assert not exists(client, f"{test_dir}/2_data.bin")
    # No temporary file is left.
    assert [i.path for i in client.dbfs.list(test_dir)] == [f"{test_dir}/1_data.bin"]


def test_dbfs_fs_router_without_legacy_dbfs(dummy_client, dummy_storage_context):
    """A workspace disabling legacy DBFS has no DBFS paths: the router refuses them."""
    dummy_storage_context.legacy_dbfs_disabled = True
    fs = DatabricksFileSystem(client=dummy_client)
    with pytest.raises(FileNotFoundError):
        fs.info("dbfs:/tmp")
    assert sorted(fs.ls("/", detail=False)) == ["/Volumes", "/Workspace"]
