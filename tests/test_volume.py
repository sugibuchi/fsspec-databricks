"""Tests of `VolumeFileSystem` and its file objects: on the dummy server, and on the live workspace."""

import asyncio
import contextlib
import errno
import pickle
import socket
from datetime import UTC, datetime, timedelta
from io import BytesIO
from random import randbytes
from unittest.mock import AsyncMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from aiohttp import (
    ClientConnectionError,
    ClientHandlerType,
    ClientRequest,
    ClientResponse,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from fsspec.asyn import sync

from fsspec_databricks import DatabricksFileSystem
from fsspec_databricks._contrib.strategy.files.fsspec_transaction import (
    AdaptiveWritableTransaction,
    TransactionFile,
)
from fsspec_databricks.path import parse_volume_path
from fsspec_databricks.volume import (
    VolumeFileSystem,
    VolumeReadableFile,
    VolumeWritableFile,
    _MultipartSession,
    _ResumableSession,
)
from tests.utils import bytes_sig, datetime_eq, init_volume_test_dir, volume_download

pytestmark = pytest.mark.backend("volume")


def dbfs_url(path: str) -> str:
    return f"dbfs:/{path.lstrip('/')}"


def stripped_dbfs_url(path: str) -> str:
    return path


def status(client: WorkspaceClient, path: str):
    """A file's metadata, or None for a directory."""
    try:
        return client.files.get_metadata(path)
    except NotFound:
        return client.files.get_directory_metadata(path)


def test_volume_fs_init():
    with VolumeFileSystem() as fs:
        assert fs.operation_concurrency == VolumeFileSystem.operation_concurrency
        assert fs.io_concurrency == VolumeFileSystem.io_concurrency

    with VolumeFileSystem(operation_concurrency=3, io_concurrency=4) as fs:
        assert fs.operation_concurrency == 3
        assert fs.io_concurrency == 4


def test_volume_fs_close():
    fs = VolumeFileSystem()
    session = sync(fs.loop, fs._get_session)

    fs.close()

    assert fs.closed
    assert session.closed

    with pytest.raises(RuntimeError):
        _ = fs.client


def test_volume_fs_serialization():
    fs = VolumeFileSystem()
    _ = sync(fs.loop, fs._get_session)

    fs2 = pickle.loads(pickle.dumps(fs))

    assert isinstance(fs2, VolumeFileSystem)
    assert fs is not fs2
    assert fs2._session is None


def test_volume_fs_pickled_as_arguments():
    fs = VolumeFileSystem(
        config_params={"host": "https://example.cloud.databricks.com", "token": "x"},
        io_concurrency=3,
    )

    fs2 = pickle.loads(pickle.dumps(fs))
    try:
        # fsspec rebuilds the file system from its constructor arguments
        assert fs2.storage_options == fs.storage_options
        assert fs2.io_concurrency == 3
    finally:
        fs.close()
        fs2.close()


def test_volume_fs_session():
    with VolumeFileSystem() as fs:
        # Created on fsspec's loop, and reused there
        session = sync(fs.loop, fs._get_session)
        assert not session.closed
        assert sync(fs.loop, fs._get_session) is session


def test_volume_fs_storage_proxy_probe_timeout(dummy_client):
    # Connections are accepted by the backlog, but no response ever comes.
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]

        with VolumeFileSystem(client=dummy_client) as fs:
            fs._storage_proxy_hostname = f"http://127.0.0.1:{port}"
            fs._storage_proxy_probe_timeout = 0.1

            assert fs.storage_proxy_available is False


def test_volume_fs_storage_proxy_probe_connection_refused(dummy_client):
    # Refused without a socket: Windows retries a refused connection for seconds, and a freed port can be reused.
    refused = ClientConnectionError("Connection refused")
    with (
        patch("aiohttp.ClientSession.head", side_effect=refused) as head,
        VolumeFileSystem(client=dummy_client) as fs,
    ):
        assert fs.storage_proxy_available is False
        head.assert_called_once()


def test_volume_fs_storage_proxy_probe_answered(dummy_client, dummy):
    dummy.storage_proxy_available = True
    with VolumeFileSystem(client=dummy_client) as fs:
        assert fs.storage_proxy_available is True
        assert fs.workspace_id == dummy.workspace_id


### Requests, on the dummy server


def _requests(dummy) -> list[tuple[str, str]]:
    """The Files API requests served, as their method and path."""
    return [
        (r.method, r.path.removeprefix("/api/2.0/fs"))
        for r in dummy.requests_to("/api/2.0/fs/")
        # The storage proxy's probe, made once per file system.
        if "/DatabricksInternal/" not in r.path
    ]


def _requests_of(dummy, func) -> list[tuple[str, str]]:
    dummy.requests.clear()
    with contextlib.suppress(OSError):
        func()
    return _requests(dummy)


def test_volume_metadata_requests(client, volume_test_root, dummy):
    """Each check of a file or a directory makes as few requests as possible."""
    test_dir = init_volume_test_dir(
        client, volume_test_root, items={"dir": {"file.bin": b"data"}}
    )
    directory = f"{test_dir}/dir"
    file = f"{directory}/file.bin"
    missing = f"{directory}/missing"
    head_file = ("HEAD", f"/files{file}")
    head_dir = ("HEAD", f"/directories{directory}")

    with VolumeFileSystem(client=client) as fs:
        # Files: one request
        assert _requests_of(dummy, lambda: fs.info(file)) == [head_file]
        assert fs.info(file)["type"] == "file"
        assert _requests_of(dummy, lambda: fs.isfile(file)) == [head_file]
        assert fs.isfile(file)
        assert not fs.isfile(directory)

        # Directories: one request to isdir, two to info
        assert _requests_of(dummy, lambda: fs.isdir(directory)) == [head_dir]
        assert fs.isdir(directory)
        assert not fs.isdir(file)
        assert _requests_of(dummy, lambda: fs.info(directory)) == [
            ("HEAD", f"/files{directory}"),
            head_dir,
        ]
        assert fs.info(directory)["type"] == "directory"

        with pytest.raises(FileNotFoundError):
            fs.info(missing)

        # Opening a directory for writing: one request
        with pytest.raises(IsADirectoryError):
            fs.open(directory, "wb")
        assert _requests_of(dummy, lambda: fs.open(directory, "wb")) == [head_dir]

        # Volumes, schemas and catalogs are directories, and never files
        volume = "/".join(directory.split("/")[:5])
        assert _requests_of(dummy, lambda: fs.isfile(volume)) == []
        assert not fs.isfile(volume)

        # Listing a directory: one request. A file lists as itself.
        assert _requests_of(dummy, lambda: fs.ls(directory)) == [
            ("GET", f"/directories{directory}")
        ]
        assert fs.ls(directory, detail=False) == [file]
        assert fs.ls(directory)[0]["size"] == len(b"data")
        assert _requests_of(dummy, lambda: fs.ls(file)) == [
            ("GET", f"/directories{file}"),
            head_file,
        ]
        assert fs.ls(file, detail=False) == [file]
        with pytest.raises(FileNotFoundError):
            fs.ls(missing)


def test_volume_cp_file_streams(client, volume_test_root, dummy):
    """Copying a file streams it with the file objects: no copy of DBFS, one metadata request each."""
    data = randbytes(12345)
    test_dir = init_volume_test_dir(client, volume_test_root, items={"src.bin": data})
    src, dst = f"{test_dir}/src.bin", f"{test_dir}/dst.bin"

    with VolumeFileSystem(client=client) as fs:
        requests = _requests_of(dummy, lambda: fs.cp_file(src, dst))
        assert volume_download(client, dst) == data
        # The source is a file, the destination no directory.
        assert [r for r in requests if r[0] == "HEAD"] == [
            ("HEAD", f"/files{src}"),
            ("HEAD", f"/directories{dst}"),
        ]
        assert dummy.requests_to("/api/2.0/dbfs/") == []

        with pytest.raises(FileNotFoundError):
            fs.cp_file(f"{test_dir}/missing", dst)


def test_volume_mv_file_streams(client, volume_test_root, dummy):
    data = randbytes(12345)
    test_dir = init_volume_test_dir(client, volume_test_root, items={"src.bin": data})

    with VolumeFileSystem(client=client) as fs:
        fs.mv(f"{test_dir}/src.bin", f"{test_dir}/dst.bin")
        assert fs.ls(test_dir, detail=False) == [f"{test_dir}/dst.bin"]
    assert volume_download(client, f"{test_dir}/dst.bin") == data
    assert dummy.requests_to("/api/2.0/dbfs/") == []


def test_volume_rm_requests(client, volume_test_root, dummy):
    """Deleting calls the Files API directly, children before their parents."""
    root = init_volume_test_dir(
        client,
        volume_test_root,
        items={
            "a.bin": b"a",
            "sub": {"b.bin": b"b", "c.bin": b"c", "empty": {}},
            "other": {},
        },
    )

    with VolumeFileSystem(client=client) as fs:
        # A file: the delete only
        assert _requests_of(dummy, lambda: fs.rm(f"{root}/a.bin")) == [
            ("DELETE", f"/files{root}/a.bin")
        ]

        with pytest.raises(IsADirectoryError):
            fs.rm(f"{root}/sub")

        # A directory: one listing per directory, then the deletes
        requests = _requests_of(dummy, lambda: fs.rm(root, recursive=True))

        assert not fs.exists(root)
        assert dummy.requests_to("/api/2.0/dbfs/") == []
        assert sorted(p for m, p in requests if m == "GET") == [
            f"/directories{root}",
            f"/directories{root}/other",
            f"/directories{root}/sub",
            f"/directories{root}/sub/empty",
        ]
        assert sorted(p for m, p in requests if m == "DELETE" and "/files" in p) == [
            f"/files{root}/sub/b.bin",
            f"/files{root}/sub/c.bin",
        ]
        deleted = [
            p.removeprefix("/directories")
            for m, p in requests
            if m == "DELETE" and p.startswith("/directories")
        ]
        assert deleted.index(f"{root}/sub/empty") < deleted.index(f"{root}/sub")
        assert deleted[-1] == root

        with pytest.raises(FileNotFoundError):
            fs.rm(root, recursive=True)


def test_volume_rm_streams_a_large_directory(client, volume_test_root, dummy):
    """A directory listing many pages is deleted as its pages come, not once all are listed."""
    root = init_volume_test_dir(client, volume_test_root)
    for i in range(2001):  # Three pages of the Files API's listing.
        dummy.vol_write(f"{root}/file-{i:04}", b"x")

    with VolumeFileSystem(client=client) as fs:
        requests = _requests_of(dummy, lambda: fs.rm(root, recursive=True))
        assert not fs.exists(root)

    pages = [
        i
        for i, (m, p) in enumerate(requests)
        if (m, p) == ("GET", f"/directories{root}")
    ]
    deletes = [
        i
        for i, (m, p) in enumerate(requests)
        if m == "DELETE" and p.startswith("/files")
    ]
    assert len(pages) == 3
    assert len(deletes) == 2001
    # The first files were deleted before the last page was listed.
    assert deletes[0] < pages[-1]


### On both APIs


def test_volume_workspace_id(client: WorkspaceClient):
    with VolumeFileSystem(client=client) as fs:
        workspace_id = fs.workspace_id
        assert isinstance(workspace_id, int)


def test_volume_storage_proxy_available(client: WorkspaceClient):
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
def test_volume_info(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)

    test_dir = init_volume_test_dir(
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
    assert info["type"] == "directory"
    if fs_class is VolumeFileSystem:
        # The router's root is the DBFS root where legacy DBFS is enabled, with its size and time.
        assert info["size"] is None
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
def test_volume_ls(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)

    test_dir = init_volume_test_dir(
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
def test_volume_exists(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_volume_test_dir(
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
def test_volume_cp_file(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)

    test_dir = init_volume_test_dir(
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
    copied_data = volume_download(client, f"{test_dir}/1_data_copied.bin")

    assert data == copied_data

    # Test copying a file larger than one block, with a multipart upload
    large_data = randbytes(12 * 1024 * 1024)
    client.files.upload(f"{test_dir}/3_large.bin", BytesIO(large_data))
    fs.cp_file(
        dbfs_url(f"{test_dir}/3_large.bin"),
        dbfs_url(f"{test_dir}/3_large_copied.bin"),
    )
    assert bytes_sig(
        volume_download(client, f"{test_dir}/3_large_copied.bin")
    ) == bytes_sig(large_data)

    # Test copying a directory
    fs.cp_file(
        dbfs_url(f"{test_dir}/2_directory"),
        dbfs_url(f"{test_dir}/2_directory_copied"),
    )

    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/2_directory_copied")

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
def test_volume_copy(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data1 = randbytes(12345)
    data2 = randbytes(12345)

    test_dir = init_volume_test_dir(
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
def test_volume_rm(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(12345)

    test_dir = init_volume_test_dir(
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
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/1_data.bin")

    with pytest.raises(IsADirectoryError):
        fs.rm(dbfs_url(f"{test_dir}/2_directory"), recursive=False)

    fs.rm(dbfs_url(f"{test_dir}/2_directory"), recursive=True)
    with pytest.raises(NotFound):
        client.files.get_directory_metadata(f"{test_dir}/2_directory")

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
def test_volume_mv(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data1 = randbytes(12345)
    data2 = randbytes(12345)

    test_dir = init_volume_test_dir(
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
    assert volume_download(client, f"{test_dir}/1_moved.bin") == data1
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/1_data.bin")

    fs.mv(
        dbfs_url(f"{test_dir}/2_directory"),
        dbfs_url(f"{test_dir}/2_moved"),
        recursive=True,
    )
    assert volume_download(client, f"{test_dir}/2_moved/3_data.bin") == data2
    with pytest.raises(NotFound):
        client.files.get_directory_metadata(f"{test_dir}/2_directory")


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_mkdir(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_volume_test_dir(client, volume_test_root, fs_class)

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
def test_volume_makedirs(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_volume_test_dir(client, volume_test_root, fs_class)

    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"))
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/parent_dir/dir")

    with pytest.raises(FileExistsError):
        fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=False)

    fs.makedirs(dbfs_url(f"{test_dir}/parent_dir/dir"), exist_ok=True)
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/parent_dir/dir")


@pytest.mark.parametrize(
    "fs_class",
    [
        VolumeFileSystem,
        DatabricksFileSystem,
    ],
)
def test_volume_rmdir(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    test_dir = init_volume_test_dir(
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
    with pytest.raises(NotFound):
        client.files.get_directory_metadata(f"{test_dir}/1_directory")

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
def test_volume_open_r(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    data = randbytes(10 * 1000 * 1000)

    test_dir = init_volume_test_dir(
        client,
        volume_test_root,
        fs_class,
        items={
            "1_data.bin": data,
            "3_directory": {},
        },
    )
    fs = fs_class(client=client)

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
def test_volume_open_w(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    fs = fs_class(client=client)

    data = randbytes(54321)

    test_dir = init_volume_test_dir(
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

    # A file no bigger than the smallest part is uploaded whole, a larger one in parts.
    for data, in_parts in [(small_data, False), (large_data, True)]:
        stream = BytesIO(data)
        with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
            while chunk := stream.read(1234 * 1000):
                f.write(chunk)

        written = volume_download(client, f"{test_dir}/1_data.bin")
        assert in_parts == (f._upload_session is not None)
        assert bytes_sig(data) == bytes_sig(written)

    # Overwrite existing file
    with fs.open(dbfs_url(f"{test_dir}/2_data.bin"), mode="wb") as f:
        f.write(small_data)

    written = volume_download(client, f"{test_dir}/2_data.bin")
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
def test_volume_open_w_abort(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    test_dir = init_volume_test_dir(client, volume_test_root, fs_class)

    with patch.object(
        VolumeWritableFile,
        "_upload_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        _abort_multipart_upload = AsyncMock()
        with patch.object(
            VolumeWritableFile, "_abort_multipart_upload", _abort_multipart_upload
        ):
            with pytest.raises(OSError):
                with fs_class(client=client) as fs:
                    with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
                        f.write(randbytes(10 * 1024 * 1024))
        assert _abort_multipart_upload.called


@pytest.mark.parametrize("fs_class", [VolumeFileSystem, DatabricksFileSystem])
def test_volume_transaction(
    client: WorkspaceClient,
    fs_class: type[VolumeFileSystem | DatabricksFileSystem],
    volume_test_root: str,
):
    test_dir = init_volume_test_dir(client, volume_test_root, fs_class)
    fs = fs_class(client=client)
    small_data = randbytes(512 * 1023)
    large_data = randbytes(10 * 1024 * 1023)

    # Commit completes the uploads, a small file's and a multipart one's, none before.
    with fs.transaction:
        for name, data in [("1_small.bin", small_data), ("2_large.bin", large_data)]:
            with fs.open(dbfs_url(f"{test_dir}/{name}"), mode="wb") as f:
                f.write(data)
            with pytest.raises(NotFound):
                client.files.get_metadata(f"{test_dir}/{name}")
    assert bytes_sig(volume_download(client, f"{test_dir}/1_small.bin")) == bytes_sig(
        small_data
    )
    assert bytes_sig(volume_download(client, f"{test_dir}/2_large.bin")) == bytes_sig(
        large_data
    )

    # An exception aborts the uploads, leaving nothing.
    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        for name, data in [("3_small.bin", small_data), ("4_large.bin", large_data)]:
            with fs.open(dbfs_url(f"{test_dir}/{name}"), mode="wb") as f:
                f.write(data)
        raise ValueError("dummy")
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/3_small.bin")
    with pytest.raises(NotFound):
        client.files.get_metadata(f"{test_dir}/4_large.bin")


### File objects, on the dummy server

path = "/Volumes/catalog_a/schema/b/volume_c/path/to/file"


async def _dummy_auth(
    request: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    return await handler(request)


def _reader(session, base_url, loop, data_size, **kwargs):
    return VolumeReadableFile(
        path,
        loop,
        data_size,
        session=session,
        base_url=base_url,
        auth=_dummy_auth,
        **kwargs,
    )


def _writer(session, base_url, loop, **kwargs):
    return VolumeWritableFile(
        path,
        loop,
        session=session,
        base_url=base_url,
        auth=_dummy_auth,
        **kwargs,
    )


def test_volume_file_read(dummy_api, dummy, client_event_loop, aiohttp_session):
    data = randbytes(20 * 1000 * 1000)
    dummy.vol_write(path, data)

    with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
        actual = f.read()

    assert bytes_sig(data) == bytes_sig(actual)

    dummy.presigned_urls_only = True

    with pytest.raises(IOError):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            prefer_presigned_url=False,
        ) as f:
            _ = f.read()


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_volume_file_read_retries(
    dummy, dummy_api, client_event_loop, aiohttp_session, status
):
    data = randbytes(1000)
    dummy.vol_write(path, data)
    dummy.read_errors.extend([status, status])

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
            assert f.read() == data

    assert not dummy.read_errors


def test_volume_file_read_retries_exhausted(
    dummy, dummy_api, client_event_loop, aiohttp_session
):
    data = randbytes(1000)
    dummy.vol_write(path, data)
    dummy.read_errors.extend([500] * 10)

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            concurrency_limit=asyncio.Semaphore(1),
        ) as f:
            with pytest.raises(OSError):
                f.read()

    # The first request and its retries.
    attempts = 10 - len(dummy.read_errors)
    assert attempts == VolumeReadableFile.fetch_max_retries + 1


@pytest.mark.parametrize("status", [400, 403, 404])
def test_volume_file_read_no_retry(
    dummy, dummy_api, client_event_loop, aiohttp_session, status
):
    data = randbytes(1000)
    dummy.vol_write(path, data)
    dummy.read_errors.extend([status] * 2)

    with patch.object(VolumeReadableFile, "fetch_retry_delay", 0):
        with _reader(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            len(data),
            concurrency_limit=asyncio.Semaphore(1),
        ) as f:
            with pytest.raises(OSError):
                f.read()

    assert len(dummy.read_errors) == 1


def test_volume_file_read_parquet(
    client_event_loop,
    dummy_api,
    dummy,
    dummy_table,
    dummy_table_parquet,
    aiohttp_session,
):
    dummy.vol_write(path, dummy_table_parquet)

    with _reader(
        aiohttp_session, dummy_api, client_event_loop, len(dummy_table_parquet)
    ) as f:
        df = pd.read_parquet(f)

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


def test_volume_file_oneshot_write(
    dummy_api, dummy, client_event_loop, aiohttp_session
):
    data = randbytes(1000 * 1000)

    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        f.write(data)

    assert bytes_sig(data) == bytes_sig(dummy.volume_files[path].data)


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_parts_write(
    dummy_api, dummy, client_event_loop, aiohttp_session, upload_mode
):
    data = randbytes(20 * 1000 * 1000)
    # The one file class follows the API the server chooses.
    dummy.upload_mode = upload_mode

    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        # Several writes, so that the parts grow
        for i in range(0, len(data), 1000 * 1000):
            f.write(data[i : i + 1000 * 1000])
        assert f.sequential is (upload_mode == "resumable")

    assert bytes_sig(data) == bytes_sig(dummy.volume_files[path].data)

    dummy.presigned_urls_only = True
    dummy.volume_files.clear()

    with pytest.raises(IOError):
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            use_presigned_url=False,
        ) as f:
            f.write(data)


def test_volume_file_resumable_one_part_in_flight(
    dummy_api, dummy, client_event_loop, aiohttp_session
):
    dummy.upload_mode = "resumable"
    data = randbytes(40 * 1000 * 1000)
    original = _ResumableSession.upload_part
    in_flight = 0
    most_in_flight = 0

    async def counting(self, *args, **kwargs):
        nonlocal in_flight, most_in_flight
        in_flight += 1
        most_in_flight = max(most_in_flight, in_flight)
        try:
            # A pause, so that a concurrent part would overlap this one.
            await asyncio.sleep(0.01)
            return await original(self, *args, **kwargs)
        finally:
            in_flight -= 1

    with patch.object(_ResumableSession, "upload_part", counting):
        with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
            for i in range(0, len(data), 1000 * 1000):
                f.write(data[i : i + 1000 * 1000])

    assert bytes_sig(data) == bytes_sig(dummy.volume_files[path].data)
    assert most_in_flight == 1


@pytest.mark.parametrize("use_presigned_url", [True, False])
def test_volume_file_unexpected_initiate_reply(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    use_presigned_url,
):
    dummy.upload_mode = "unknown"

    with pytest.raises(OSError, match="Unexpected initiate-upload response"):
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            use_presigned_url=use_presigned_url,
        ) as f:
            f.write(randbytes(20 * 1000 * 1000))

    assert path not in dummy.volume_files


@pytest.mark.parametrize("name", ["min_block_size", "max_block_size"])
def test_volume_file_part_granularity(
    dummy_api, client_event_loop, aiohttp_session, name
):
    with pytest.raises(ValueError, match="multiple of"):
        _writer(
            aiohttp_session, dummy_api, client_event_loop, **{name: 10 * 1024 * 1023}
        )


@pytest.mark.parametrize("file_class", [VolumeReadableFile, VolumeWritableFile])
def test_volume_file_invalid_spread(
    dummy_api, client_event_loop, aiohttp_session, file_class
):
    args = (1,) if file_class is VolumeReadableFile else ()

    with pytest.raises(ValueError):
        file_class(
            path,
            client_event_loop,
            *args,
            session=aiohttp_session,
            base_url=dummy_api,
            auth=_dummy_auth,
            spread=0,
        )


@pytest.mark.parametrize("abort_supported", [True, False])
def test_volume_file_multipart_write_abort(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    abort_supported,
):
    dummy.abort_supported = abort_supported

    with patch.object(
        VolumeWritableFile,
        "_upload_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        with pytest.raises(OSError) as exc_info:
            with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
                f.write(randbytes(20 * 1000 * 1000))

    # The upload's failure is raised, not the abort's
    assert "Failed to upload a part" in str(exc_info.value)
    assert path not in dummy.volume_files

    if abort_supported:
        assert dummy.multipart_uploads == {}


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_write_parquet(
    client_event_loop,
    dummy_api,
    dummy,
    dummy_table,
    upload_mode,
    aiohttp_session,
):
    dummy.upload_mode = upload_mode

    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        pq.write_table(pa.Table.from_pylist(dummy_table), f)

    df = pd.read_parquet(BytesIO(dummy.volume_files[path].data))

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


@pytest.mark.parametrize("use_presigned_url", [True, False])
@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_volume_file_parts_write_abort_per_mode(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    upload_mode,
    use_presigned_url,
):
    dummy.upload_mode = upload_mode
    session_class = (
        _MultipartSession if upload_mode == "multipart" else _ResumableSession
    )

    with patch.object(
        session_class,
        "upload_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        with pytest.raises(OSError, match="Failed to upload a part"):
            with _writer(
                aiohttp_session,
                dummy_api,
                client_event_loop,
                use_presigned_url=use_presigned_url,
            ) as f:
                f.write(randbytes(20 * 1000 * 1000))

    assert path not in dummy.volume_files
    # The session the server started is aborted by its own API.
    assert dummy.multipart_uploads == {}
    assert dummy.resumable_uploads == {}


def test_volume_file_abort_without_session(
    dummy_api, dummy, client_event_loop, aiohttp_session
):
    with patch.object(
        VolumeWritableFile,
        "_post_json",
        AsyncMock(side_effect=AssertionError("No request expected")),
    ):
        f = _writer(aiohttp_session, dummy_api, client_event_loop)
        # No session started: the abort sends nothing.
        asyncio.run_coroutine_threadsafe(
            f._abort_multipart_upload(), client_event_loop
        ).result()


@pytest.mark.parametrize("size", [1000, 20 * 1000 * 1000])
@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
@pytest.mark.parametrize("commit", [True, False])
def test_volume_file_transaction(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    upload_mode,
    size,
    commit,
):
    """In a transaction, nothing is visible before commit, and discard leaves nothing."""
    dummy.upload_mode = upload_mode
    data = randbytes(size)
    f = TransactionFile(
        AdaptiveWritableTransaction(
            _writer(aiohttp_session, dummy_api, client_event_loop), hold_last_part=True
        )
    )
    with f:
        for i in range(0, len(data), 1000 * 1000):
            f.write(data[i : i + 1000 * 1000])
    assert path not in dummy.volume_files

    if commit:
        f.commit()
        assert bytes_sig(data) == bytes_sig(dummy.volume_files[path].data)
    else:
        f.discard()
        assert path not in dummy.volume_files
        assert dummy.multipart_uploads == {}
        if upload_mode == "multipart":
            assert dummy.resumable_uploads == {}
