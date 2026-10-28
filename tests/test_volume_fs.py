import asyncio
import contextlib
import errno
import inspect
import logging
import pickle
from datetime import UTC, datetime, timedelta
from io import BytesIO
from random import randbytes
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound, ResourceDoesNotExist
from databricks.sdk.service.files import DirectoryEntry, GetMetadataResponse

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.path import parse_volume_path
from fsspec_databricks.volume import (
    VolumeFileSystem,
    VolumeReadableFile,
    VolumeWritableFile,
)
from tests.utils import bytes_sig, datetime_eq

log = logging.getLogger(__name__)


def init_test_dir(
    client: WorkspaceClient,
    test_root: str,
    fs_class: type | None = None,
    name: str | None = None,
    items: dict[str, dict | bytes] | None = None,
) -> str:
    """Initialize a test directory in the Unity Catalog volume with the given items."""
    name = name or inspect.stack()[1].function

    if fs_class:
        test_dir = f"{test_root}/{name}_{fs_class.__name__}"
    else:
        test_dir = f"{test_root}/{name}"

    try:
        log.debug("Checking test dir: path=%s", test_dir)
        client.files.get_directory_metadata(test_dir)
        log.debug("Deleting test dir: path=%s", test_dir)
        client.dbfs.delete(test_dir, recursive=True)
        log.debug("Deleted test dir: path=%s", test_dir)
    except NotFound:
        pass

    log.debug("Creating test dir: path=%s", test_dir)
    client.files.create_directory(test_dir)
    log.debug("Created test dir: path=%s", test_dir)

    def init(path: str, _items: dict[str, dict | bytes]):
        for n, i in _items.items():
            item_path = f"{path}/{n}"
            if isinstance(i, dict):
                client.files.create_directory(item_path)
                init(item_path, i)
            else:
                client.files.upload(item_path, BytesIO(i))

    if items:
        log.debug("Creating files/directories in the test dir: path=%s", test_dir)
        init(test_dir, items)
        log.debug("Created files/directories in the test dir: path=%s", test_dir)

    return test_dir


def dbfs_url(path: str) -> str:
    return f"dbfs:/{path.lstrip('/')}"


def stripped_dbfs_url(path: str) -> str:
    return path


def status(client: WorkspaceClient, path: str) -> GetMetadataResponse | None:
    """Get the status of the file or directory at the given path in the Databricks workspace."""
    try:
        return client.files.get_metadata(path)
    except NotFound:
        return client.files.get_directory_metadata(path)


def download(client: WorkspaceClient, path: str) -> bytes:
    """Download the file at the given path from the Databricks workspace."""
    with client.files.download(path).contents as f:
        return f.read()


def test_volume_fs_init():
    with VolumeFileSystem() as fs:
        assert fs.max_read_concurrency == VolumeFileSystem.max_write_concurrency
        assert fs.min_read_block_size == VolumeFileSystem.min_read_block_size
        assert fs.max_read_block_size == VolumeFileSystem.max_read_block_size

        assert fs.max_write_concurrency == VolumeFileSystem.max_write_concurrency
        assert fs.min_write_block_size == VolumeFileSystem.min_write_block_size
        assert fs.max_write_block_size == VolumeFileSystem.max_write_block_size

        assert (
            fs.min_multipart_upload_size == VolumeFileSystem.min_multipart_upload_size
        )

    with VolumeFileSystem(
        max_read_concurrency=1,
        min_read_block_size=2 * 1024 * 1024,
        max_read_block_size=3 * 1024 * 1024,
        max_write_concurrency=4,
        min_write_block_size=5 * 1024 * 1024,
        max_write_block_size=6 * 1024 * 1024,
        min_multipart_upload_size=7 * 1024 * 1024,
    ) as fs:
        assert fs.max_read_concurrency == 1
        assert fs.min_read_block_size == 2 * 1024 * 1024
        assert fs.max_read_block_size == 3 * 1024 * 1024

        assert fs.max_write_concurrency == 4
        assert fs.min_write_block_size == 5 * 1024 * 1024
        assert fs.max_write_block_size == 6 * 1024 * 1024

        assert fs.min_multipart_upload_size == 7 * 1024 * 1024


def test_volume_fs_close():
    fs = VolumeFileSystem(verbose_debug_log=True)
    loop = fs._loop

    fs.close()

    assert fs.closed
    assert loop.is_closed()

    with pytest.raises(RuntimeError):
        _ = fs._loop


def test_volume_fs_serialization():
    fs = VolumeFileSystem()
    _ = fs._loop

    serialized = pickle.dumps(fs)
    fs2 = pickle.loads(serialized)

    assert isinstance(fs2, VolumeFileSystem)
    assert fs is not fs2
    assert fs._loop is not fs2._loop


def test_volume_fs_pickled_as_arguments():
    fs = VolumeFileSystem(
        config_params={"host": "https://example.cloud.databricks.com", "token": "x"},
        max_read_concurrency=3,
    )
    loop = fs._loop

    fs2 = pickle.loads(pickle.dumps(fs))
    try:
        # fsspec rebuilds the file system from its constructor arguments
        assert fs2.storage_options == fs.storage_options
        assert fs2.max_read_concurrency == 3
        assert fs2._loop is not loop
    finally:
        fs.close()
        fs2.close()


def test_volume_fs_session():
    with VolumeFileSystem() as fs:
        # Created from a thread without a running event loop
        session = fs._session
        assert not session.closed
        assert session is fs._session

        async def get_session():
            return fs._session

        # Accessed from the IO event loop
        assert (
            asyncio.run_coroutine_threadsafe(get_session(), fs._loop).result(timeout=5)
            is session
        )


def test_volume_fs_session_in_event_loop():
    with VolumeFileSystem() as fs:

        async def get_session():
            return fs._session

        # Created from the IO event loop without deadlock
        session = asyncio.run_coroutine_threadsafe(get_session(), fs._loop).result(
            timeout=5
        )
        assert not session.closed
        assert session is fs._session


class _CountingFilesAPI:
    """A fake of `WorkspaceClient.files` for metadata, listing and deletion, recording the calls."""

    def __init__(self, files: dict[str, bytes], directories: list[str]):
        self.files = files
        self.directories = directories
        self.calls: list[tuple[str, str]] = []

    def get_metadata(self, path):
        self.calls.append(("file", path))
        if path not in self.files:
            raise NotFound(f"Not found: {path}")
        return GetMetadataResponse(
            content_length=len(self.files[path]),
            content_type="application/octet-stream",
            last_modified="Wed, 21 Oct 2026 07:28:00 GMT",
        )

    def get_directory_metadata(self, path):
        self.calls.append(("directory", path))
        if path not in self.directories:
            raise NotFound(f"Not found: {path}")

    def _children(self, path):
        return [
            p for p in [*self.files, *self.directories] if p.rsplit("/", 1)[0] == path
        ]

    def list_directory_contents(self, path):
        self.calls.append(("list", path))
        if path not in self.directories:
            raise NotFound(f"Not found: {path}")
        for p in self._children(path):
            yield DirectoryEntry(
                path=p,
                name=p.rsplit("/", 1)[1],
                is_directory=p in self.directories,
                file_size=len(self.files[p]) if p in self.files else None,
            )

    def delete(self, path):
        self.calls.append(("delete", path))
        if path not in self.files:
            raise NotFound(f"Not found: {path}")
        del self.files[path]

    def delete_directory(self, path):
        self.calls.append(("delete_directory", path))
        if path not in self.directories:
            raise NotFound(f"Not found: {path}")
        if self._children(path):
            raise BadRequest(f"Directory {path} is not empty")
        self.directories.remove(path)


def test_volume_fs_metadata_calls():
    """Each check of a file or a directory makes as few API calls as possible."""
    file = "/Volumes/c/s/v/dir/file.bin"
    directory = "/Volumes/c/s/v/dir"
    missing = "/Volumes/c/s/v/dir/missing"

    files_api = _CountingFilesAPI(files={file: b"data"}, directories=[directory])
    client = MagicMock()
    client.files = files_api

    def calls_of(func):
        files_api.calls.clear()
        with contextlib.suppress(OSError):
            func()
        return list(files_api.calls)

    with VolumeFileSystem(client=client) as fs:
        # Files: one call
        assert calls_of(lambda: fs.info(file)) == [("file", file)]
        assert fs.info(file)["type"] == "file"
        assert calls_of(lambda: fs.isfile(file)) == [("file", file)]
        assert fs.isfile(file)
        assert not fs.isfile(directory)

        # Directories: one call to isdir, two to info
        assert calls_of(lambda: fs.isdir(directory)) == [("directory", directory)]
        assert fs.isdir(directory)
        assert not fs.isdir(file)
        assert calls_of(lambda: fs.info(directory)) == [
            ("file", directory),
            ("directory", directory),
        ]
        assert fs.info(directory)["type"] == "directory"

        # Missing paths
        with pytest.raises(FileNotFoundError):
            fs.info(missing)

        # Opening a directory for writing: one call
        with pytest.raises(IsADirectoryError):
            fs.open(directory, "wb")
        assert calls_of(lambda: fs.open(directory, "wb")) == [("directory", directory)]

        # Volumes, schemas and catalogs are directories, and never files
        assert calls_of(lambda: fs.isfile("/Volumes/c/s/v")) == []
        assert not fs.isfile("/Volumes/c/s/v")

        # Listing a directory: one call. A file lists as itself.
        assert calls_of(lambda: fs.ls(directory)) == [("list", directory)]
        assert fs.ls(directory, detail=False) == [file]
        assert fs.ls(directory)[0]["size"] == len(b"data")
        assert calls_of(lambda: fs.ls(file)) == [("list", file), ("file", file)]
        assert fs.ls(file, detail=False) == [file]
        with pytest.raises(FileNotFoundError):
            fs.ls(missing)


def _volume_fs_with_fake_files(
    files: dict[str, bytes], directories: list[str]
) -> tuple[VolumeFileSystem, _CountingFilesAPI, MagicMock]:
    """A Volume file system on `_CountingFilesAPI`, whose files are opened as `BytesIO`."""
    files_api = _CountingFilesAPI(files=files, directories=directories)
    client = MagicMock()
    client.files = files_api

    class _Written(BytesIO):
        def __init__(self, path):
            super().__init__()
            self.path = path

        def close(self):
            files_api.files[self.path] = self.getvalue()
            super().close()

    def fake_open(fs, path, mode="rb", **kwargs):
        fs._check_open_target(path, mode)
        return BytesIO(files_api.files[path]) if "r" in mode else _Written(path)

    patcher = patch.object(VolumeFileSystem, "_open", fake_open)
    patcher.start()
    fs = VolumeFileSystem(client=client)
    fs._fake_patcher = patcher
    return fs, files_api, client


@pytest.fixture
def fake_volume_fs():
    created = []

    def create(files: dict[str, bytes], directories: list[str]):
        fs, files_api, client = _volume_fs_with_fake_files(files, directories)
        created.append(fs)
        return fs, files_api, client

    yield create

    for fs in created:
        fs._fake_patcher.stop()
        fs.close()


def test_volume_fs_cp_file_streams(fake_volume_fs):
    """Copying a file streams it with the file objects, not the SDK's `dbfs.copy`."""
    src = "/Volumes/c/s/v/dir/src.bin"
    dst = "/Volumes/c/s/v/dir/dst.bin"
    data = randbytes(12345)

    fs, files_api, client = fake_volume_fs({src: data}, ["/Volumes/c/s/v/dir"])
    fs.cp_file(src, dst)

    assert files_api.files[dst] == data
    # One metadata call each: the source is a file, the destination is no directory
    assert files_api.calls == [("file", src), ("directory", dst)]
    assert client.dbfs.mock_calls == []

    with pytest.raises(FileNotFoundError):
        fs.cp_file("/Volumes/c/s/v/dir/missing", dst)


def test_volume_fs_mv_file(fake_volume_fs):
    src = "/Volumes/c/s/v/dir/src.bin"
    dst = "/Volumes/c/s/v/dir/dst.bin"
    data = randbytes(12345)

    fs, files_api, client = fake_volume_fs({src: data}, ["/Volumes/c/s/v/dir"])
    fs.mv(src, dst)

    assert files_api.files == {dst: data}
    assert client.dbfs.mock_calls == []


def test_volume_fs_rm_calls(fake_volume_fs):
    """Deleting calls the Files API directly, children before their parents."""
    root = "/Volumes/c/s/v/dir"
    files = {
        f"{root}/a.bin": b"a",
        f"{root}/sub/b.bin": b"b",
        f"{root}/sub/c.bin": b"c",
    }
    directories = [root, f"{root}/sub", f"{root}/sub/empty", f"{root}/other"]

    fs, files_api, client = fake_volume_fs(dict(files), list(directories))

    # A file: one metadata call and the delete
    fs.rm(f"{root}/a.bin")
    assert files_api.calls == [("file", f"{root}/a.bin"), ("delete", f"{root}/a.bin")]

    with pytest.raises(IsADirectoryError):
        fs.rm(f"{root}/sub")

    # A directory: one listing per directory, then the deletes
    files_api.calls.clear()
    fs.rm(root, recursive=True)

    assert files_api.files == {}
    assert files_api.directories == []
    assert client.dbfs.mock_calls == []

    kinds = [kind for kind, _ in files_api.calls]
    assert kinds.count("list") == 4
    assert sorted(p for kind, p in files_api.calls if kind == "delete") == [
        f"{root}/sub/b.bin",
        f"{root}/sub/c.bin",
    ]
    deleted_directories = [
        p for kind, p in files_api.calls if kind == "delete_directory"
    ]
    assert deleted_directories.index(f"{root}/sub/empty") < deleted_directories.index(
        f"{root}/sub"
    )
    assert deleted_directories[-1] == root

    with pytest.raises(FileNotFoundError):
        fs.rm(root, recursive=True)


def test_volume_fs_workspace_id(client: WorkspaceClient):
    with VolumeFileSystem(client=client) as fs:
        workspace_id = fs.workspace_id
        assert isinstance(workspace_id, int)


def test_volume_fs_storage_proxy_available(client: WorkspaceClient):
    with VolumeFileSystem(client=client) as fs:
        available = fs.storage_proxy_available
        assert isinstance(available, bool)


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_info(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "data.bin": data,
            "directory": {},
        },
    )

    data_path = f"{test_dir}/data.bin"
    info = fs.info(dbfs_url(data_path))

    assert info["name"] == stripped_dbfs_url(data_path)
    assert info["size"] == len(data)
    assert info["type"] == "file"
    assert info["created"] is None
    assert datetime.now(tz=UTC) - info["modified"] < timedelta(minutes=1)
    assert info["islink"] is False
    assert info["content-type"] == "application/octet-stream"
    assert fs.created(dbfs_url(data_path)) == info["created"]
    assert fs.modified(dbfs_url(data_path)) == info["modified"]

    dir_path = f"{test_dir}/directory"
    info = fs.info(dbfs_url(dir_path))

    assert info["name"] == stripped_dbfs_url(dir_path)
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] is None
    assert info["modified"] is None
    assert info["islink"] is False
    assert "content-type" not in info

    with pytest.raises(FileNotFoundError):
        fs.info(dbfs_url(f"{test_dir}/nonexistent"))

    catalog, schema, volume, _, _ = parse_volume_path(test_dir)

    info = fs.info("/")
    assert info["name"] == "/"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] is None
    assert info["modified"] is None
    assert info["islink"] is False

    info = fs.info("/Volumes")
    assert info["name"] == "/Volumes"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] is None
    assert info["modified"] is None
    assert info["islink"] is False

    info = fs.info(f"/Volumes/{catalog}")
    assert info["name"] == f"/Volumes/{catalog}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False

    info = fs.info(f"/Volumes/{catalog}/{schema}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False

    info = fs.info(f"/Volumes/{catalog}/{schema}/{volume}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}/{volume}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_ls(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data,
            "2_directory": {},
        },
    )

    items: list[dict] = fs.ls(stripped_dbfs_url(test_dir), detail=True)

    assert len(items) == 2
    items.sort(key=lambda i: i["name"])

    data_path = f"{test_dir}/1_data.bin"
    assert items[0]["name"] == stripped_dbfs_url(data_path)
    assert items[0]["size"] == len(data)
    assert items[0]["type"] == "file"
    assert items[0]["created"] is None
    assert datetime.now(tz=UTC) - items[0]["modified"] < timedelta(minutes=1)
    assert items[0]["islink"] is False
    assert datetime_eq(fs.created(dbfs_url(data_path)), items[0]["created"])
    assert datetime_eq(fs.modified(dbfs_url(data_path)), items[0]["modified"])

    dir_path = f"{test_dir}/2_directory"
    assert items[1]["name"] == stripped_dbfs_url(dir_path)
    assert items[1]["size"] is None
    assert items[1]["type"] == "directory"
    assert items[1]["created"] is None
    assert items[1]["modified"] is None
    assert items[1]["islink"] is False
    assert "content-type" not in items[1]

    assert fs.ls(stripped_dbfs_url(test_dir), detail=False) == [
        stripped_dbfs_url(f"{test_dir}/1_data.bin"),
        stripped_dbfs_url(f"{test_dir}/2_directory"),
    ]

    # A file lists as itself
    assert fs.ls(dbfs_url(data_path), detail=False) == [stripped_dbfs_url(data_path)]

    with pytest.raises(FileNotFoundError):
        fs.ls(dbfs_url(f"{test_dir}/nonexistent"))

    catalog, schema, volume, _, _ = parse_volume_path(test_dir)

    if fs_class == VolumeFileSystem:
        items = fs.ls("/")
        assert len(items) == 1

        info = items[0]
        assert info["name"] == "/Volumes"
        assert info["size"] is None
        assert info["type"] == "directory"
        assert info["created"] is None
        assert info["modified"] is None
        assert info["islink"] is False

        assert fs.ls("/", detail=False) == ["/Volumes"]

    items = fs.ls("/Volumes")
    assert len(items) >= 1

    info: dict = next(i for i in items if i["name"] == f"/Volumes/{catalog}")
    assert info["name"] == f"/Volumes/{catalog}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False

    assert f"/Volumes/{catalog}" in fs.ls("/Volumes", detail=False)

    items = fs.ls(f"/Volumes/{catalog}")
    assert len(items) >= 1

    info: dict = next(i for i in items if i["name"] == f"/Volumes/{catalog}/{schema}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False

    assert f"/Volumes/{catalog}/{schema}" in fs.ls(f"/Volumes/{catalog}", detail=False)

    items = fs.ls(f"/Volumes/{catalog}/{schema}")
    assert len(items) >= 1

    info: dict = next(
        i for i in items if i["name"] == f"/Volumes/{catalog}/{schema}/{volume}"
    )
    assert info["name"] == f"/Volumes/{catalog}/{schema}/{volume}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=UTC)
    assert info["islink"] is False

    assert f"/Volumes/{catalog}/{schema}/{volume}" in fs.ls(
        f"/Volumes/{catalog}/{schema}", detail=False
    )


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_exists(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": randbytes(12345),
            "2_directory": {},
        },
    )

    assert fs.exists(dbfs_url(f"{test_dir}/1_data.bin"))
    assert fs.exists(dbfs_url(f"{test_dir}/2_directory"))
    assert not fs.exists(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_cp_file(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data,
            "2_directory": {},
        },
    )

    # Test copying a file
    fs.cp_file(
        dbfs_url(f"{test_dir}/1_data.bin"),
        dbfs_url(f"{test_dir}/1_data_copied.bin"),
    )
    copied_data = download(client, f"{test_dir}/1_data_copied.bin")

    assert data == copied_data

    # Test copying a file larger than one block, with a multipart upload
    large_data = randbytes(12 * 1024 * 1024)
    client.files.upload(f"{test_dir}/3_large.bin", BytesIO(large_data))
    fs.cp_file(
        dbfs_url(f"{test_dir}/3_large.bin"),
        dbfs_url(f"{test_dir}/3_large_copied.bin"),
    )
    assert bytes_sig(download(client, f"{test_dir}/3_large_copied.bin")) == bytes_sig(
        large_data
    )

    # Test copying a directory
    fs.cp_file(
        dbfs_url(f"{test_dir}/2_directory"),
        dbfs_url(f"{test_dir}/2_directory_copied"),
    )

    assert status(client, f"{test_dir}/2_directory_copied") is None

    # Test copying not existent path
    with pytest.raises(FileNotFoundError):
        fs.cp_file(
            dbfs_url(f"{test_dir}/4_nonexistent"),
            dbfs_url(f"{test_dir}/4_nonexistent_copy"),
        )


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_copy(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data1 = randbytes(12345)
    data2 = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "directory": {
                "1_data.bin": data1,
                "2_directory": {
                    "3_data.bin": data2,
                },
                "4_empty_directory": {},
            }
        },
    )

    fs.copy(
        dbfs_url(f"{test_dir}/directory"),
        dbfs_url(f"{test_dir}/directory_copied"),
        recursive=True,
    )

    items = [
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(test_dir)
    ]
    assert items == [
        (f"{test_dir}/directory", True),
        (f"{test_dir}/directory_copied", True),
    ]

    items = [
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(f"{test_dir}/directory_copied")
    ]
    assert items == [
        (f"{test_dir}/directory_copied/1_data.bin", False),
        (f"{test_dir}/directory_copied/2_directory", True),
        (f"{test_dir}/directory_copied/4_empty_directory", True),
    ]

    items = [
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(
            f"{test_dir}/directory_copied/2_directory"
        )
    ]
    assert items == [
        (f"{test_dir}/directory_copied/2_directory/3_data.bin", False),
    ]

    items = [
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(
            f"{test_dir}/directory_copied/4_empty_directory"
        )
    ]
    assert len(items) == 0


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_rm(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data,
            "2_directory": {
                "3_data.bin": data,
                "4_directory": {
                    "5_data.bin": data,
                    "6_empty_directory": {},
                },
            },
            "7_data.bin": data,
        },
    )

    fs.rm(dbfs_url(f"{test_dir}/1_data.bin"))
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/1_data.bin")

    with pytest.raises(IsADirectoryError):
        fs.rm(dbfs_url(f"{test_dir}/2_directory"), recursive=False)

    fs.rm(dbfs_url(f"{test_dir}/2_directory"), recursive=True)
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/2_directory")

    # A file, recursively
    fs.rm(dbfs_url(f"{test_dir}/7_data.bin"), recursive=True)
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/7_data.bin")

    with pytest.raises(FileNotFoundError):
        fs.rm(dbfs_url(f"{test_dir}/nonexistent"))


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_mv(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data1 = randbytes(12345)
    data2 = randbytes(12345)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data1,
            "2_directory": {
                "3_data.bin": data2,
            },
        },
    )

    fs.mv(dbfs_url(f"{test_dir}/1_data.bin"), dbfs_url(f"{test_dir}/1_moved.bin"))
    assert download(client, f"{test_dir}/1_moved.bin") == data1
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/1_data.bin")

    fs.mv(
        dbfs_url(f"{test_dir}/2_directory"),
        dbfs_url(f"{test_dir}/2_moved"),
        recursive=True,
    )
    assert download(client, f"{test_dir}/2_moved/3_data.bin") == data2
    with pytest.raises(NotFound):
        client.files.get_directory_metadata(f"{test_dir}/2_directory")


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_mkdir(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(client, volume_test_root, fs_class)

    fs.mkdir(dbfs_url(f"{test_dir}/test_dir"))

    status1 = status(client, f"{test_dir}/test_dir")
    assert status1 is None  # directory

    with pytest.raises(FileExistsError):
        fs.mkdir(dbfs_url(f"{test_dir}/test_dir"))

    with pytest.raises(FileNotFoundError):
        fs.mkdir(dbfs_url(f"{test_dir}/parent_dir/dir"), create_parents=False)

    fs.mkdir(dbfs_url(f"{test_dir}/parent_dir/dir"), create_parents=True)
    status2 = status(client, f"{test_dir}/parent_dir/dir")

    assert status2 is None  # directory


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_makedirs(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(client, volume_test_root, fs_class)

    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"))
    assert status(client, f"{test_dir}/parent_dir/dir") is None

    with pytest.raises(FileExistsError):
        fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=False)

    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=True)
    assert status(client, f"{test_dir}/parent_dir/dir") is None


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_rmdir(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_directory": {},
            "2_data.bin": randbytes(12345),
            "3_nonempty_directory": {
                "4_data.bin": randbytes(12345),
            },
        },
    )

    fs.rmdir(dbfs_url(f"{test_dir}/1_directory"))
    with pytest.raises(ResourceDoesNotExist):
        client.workspace.get_status(f"{test_dir}/1_directory")

    with pytest.raises(FileNotFoundError):
        fs.rmdir(dbfs_url(f"{test_dir}/nonexistent"))

    with pytest.raises(NotADirectoryError):
        fs.rmdir(dbfs_url(f"{test_dir}/2_data.bin"))

    with pytest.raises(OSError) as exc_info:
        fs.rmdir(dbfs_url(f"{test_dir}/3_nonempty_directory"))
    assert exc_info.value.errno == errno.ENOTEMPTY


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_open_r(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    data = randbytes(10 * 1000 * 1000)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data,
            "3_directory": {},
        },
    )
    fs = fs_class(client=client, verbose_debug_log=True)

    for min_size, use_presigned_url in [(len(data), True), (len(data) + 1, False)]:
        with patch.object(VolumeReadableFile, "min_presigned_url_size", min_size):
            read_data = bytearray()
            with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="rb") as f:
                while chunk := f.read(1234 * 1000):
                    read_data.extend(chunk)

            assert f.use_presigned_url == use_presigned_url
            assert bytes_sig(data) == bytes_sig(read_data)

    with pytest.raises(IsADirectoryError):
        fs.open(dbfs_url(f"{test_dir}/3_directory"), mode="rb")

    with pytest.raises(FileNotFoundError):
        fs.open(dbfs_url(f"{test_dir}/nonexistent"), mode="rb")


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_open_w(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client, verbose_debug_log=True)

    data = randbytes(54321)

    test_dir = init_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "2_data.bin": data,
            "3_directory": {},
        },
    )

    small_data = randbytes(512 * 1023)
    large_data = randbytes(10 * 1024 * 1023)

    for data, min_size, has_session_token in [
        (small_data, len(small_data) - 1, False),
        (large_data, len(large_data), True),
    ]:
        with patch.object(
            VolumeWritableFile, "min_multipart_upload_size", min_size
        ) as _upload_part:
            stream = BytesIO(data)
            with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
                while chunk := stream.read(1234 * 1000):
                    f.write(chunk)

            written = download(client, f"{test_dir}/1_data.bin")
            assert has_session_token == (f._session_token is not None)
            assert bytes_sig(data) == bytes_sig(written)

    # Overwrite existing file
    with fs.open(dbfs_url(f"{test_dir}/2_data.bin"), mode="wb") as f:
        f.write(small_data)

    written = download(client, f"{test_dir}/2_data.bin")
    assert bytes_sig(small_data) == bytes_sig(written)

    with pytest.raises(IsADirectoryError):
        with fs.open(dbfs_url(f"{test_dir}/3_directory"), mode="wb") as f:
            f.write(small_data)


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_open_w_invalid_block_size(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    test_dir = init_test_dir(client, volume_test_root, fs_class)

    with fs_class(client=client, verbose_debug_log=True) as fs:
        with pytest.raises(ValueError):
            fs.open(
                dbfs_url(f"{test_dir}/1_data.bin"),
                mode="wb",
                min_block_size=10 * 1024 * 1023,
            )

        with pytest.raises(ValueError):
            fs.open(
                dbfs_url(f"{test_dir}/1_data.bin"),
                mode="wb",
                max_block_size=10 * 1024 * 1023,
            )


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_fs_open_w_abort(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    test_dir = init_test_dir(client, volume_test_root, fs_class)

    with patch.object(
        VolumeWritableFile, "_upload_part", side_effect=RuntimeError("Dummy")
    ):
        with patch.object(
            VolumeWritableFile, "_abort_multipart_upload"
        ) as _abort_multipart_upload:
            with pytest.raises(OSError):
                with fs_class(client=client, verbose_debug_log=True) as fs:
                    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
                        f.write(randbytes(10 * 1024 * 1024))
        assert _abort_multipart_upload.called
