import errno
import inspect
import logging
import pickle
from datetime import datetime, timedelta, timezone
from io import BytesIO
from random import randbytes

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.files import GetMetadataResponse

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks.path import parse_volume_path
from fsspec_databricks.volume import VolumeFileSystem

from .utils import bytes_sig

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
    assert datetime.now(tz=timezone.utc) - info["modified"] < timedelta(minutes=1)
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
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["islink"] is False

    info = fs.info(f"/Volumes/{catalog}/{schema}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["islink"] is False

    info = fs.info(f"/Volumes/{catalog}/{schema}/{volume}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}/{volume}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
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
    assert datetime.now(tz=timezone.utc) - items[0]["modified"] < timedelta(minutes=1)
    assert items[0]["islink"] is False
    assert fs.created(dbfs_url(data_path)) == items[0]["created"]
    assert fs.modified(dbfs_url(data_path)) == items[0]["modified"]

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
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["islink"] is False

    assert f"/Volumes/{catalog}" in fs.ls("/Volumes", detail=False)

    items = fs.ls(f"/Volumes/{catalog}")
    assert len(items) >= 1

    info: dict = next(i for i in items if i["name"] == f"/Volumes/{catalog}/{schema}")
    assert info["name"] == f"/Volumes/{catalog}/{schema}"
    assert info["size"] is None
    assert info["type"] == "directory"
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
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
    assert info["created"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert info["modified"] > datetime(2020, 1, 1, tzinfo=timezone.utc)
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
            },
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

    with pytest.raises(FileNotFoundError):
        fs.rm(dbfs_url(f"{test_dir}/nonexistent"))


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

    read_data = bytearray()
    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="rb") as f:
        while chunk := f.read(1234 * 1000):
            read_data.extend(chunk)

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

    written_data = randbytes(10 * 1000 * 1000)
    stream = BytesIO(written_data)
    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
        while chunk := stream.read(1234 * 1000):
            f.write(chunk)

    read_data = download(client, f"{test_dir}/1_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    # Overwrite existing file
    with fs.open(dbfs_url(f"{test_dir}/2_data.bin"), mode="wb") as f:
        f.write(written_data)

    read_data = download(client, f"{test_dir}/2_data.bin")
    assert bytes_sig(written_data) == bytes_sig(read_data)

    with pytest.raises(IsADirectoryError):
        with fs.open(dbfs_url(f"{test_dir}/3_directory"), mode="wb") as f:
            f.write(written_data)
