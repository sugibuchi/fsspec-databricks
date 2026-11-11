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
    _FILES_API_V2,
    _FILES_API_V3,
    _IO_OFFLOAD,
    VolumeFileSystem,
    VolumeReadableFile,
    VolumeWritableFile,
    _Endpoint,
    _parse_endpoint,
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


def _defaults(host: str) -> list[_Endpoint]:
    return [
        _Endpoint(_IO_OFFLOAD, host, multipart_upload_supported=True),
        _Endpoint(_FILES_API_V2, host, multipart_upload_supported=False),
    ]


def test_volume_fs_endpoints_not_served(dummy_client, dummy, dummy_api):
    """Without the endpoints API, the SDK's defaults: asked once per file system."""
    with VolumeFileSystem(client=dummy_client) as fs:
        assert sync(fs.loop, fs._get_endpoints) == _defaults(dummy_api)
        assert sync(fs.loop, fs._get_endpoints) == _defaults(dummy_api)
    assert len(dummy.requests_to("/api/2.0/fs/endpoints")) == 1


def test_volume_fs_endpoints_unreachable(dummy_client, dummy_api):
    # Refused without a socket: Windows retries a refused connection for seconds, and a freed port can be reused.
    refused = ClientConnectionError("Connection refused")
    with (
        patch("aiohttp.ClientSession.get", side_effect=refused) as get,
        VolumeFileSystem(client=dummy_client) as fs,
    ):
        assert sync(fs.loop, fs._get_endpoints) == _defaults(dummy_api)
        get.assert_called_once()


def test_volume_fs_endpoints_timeout(dummy_client, dummy_api):
    # Connections are accepted by the backlog, but no response ever comes.
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
        with VolumeFileSystem(client=dummy_client) as fs:
            fs._endpoints_timeout = 0.1
            endpoints = sync(fs.loop, fs._fetch_endpoints, f"http://127.0.0.1:{port}")
            assert endpoints == []


def test_volume_fs_endpoints_listed(dummy_client, dummy, dummy_api):
    """The usable entries, in order; none usable gives the defaults."""
    host = dummy_api.removeprefix("http://")
    dummy.endpoints = [
        {"api": "UNKNOWN", "scheme": "http", "hostname": host},
        {"api": "FILES_API", "api_version": "3.0", "scheme": "http", "hostname": host},
        {"api": "IO_OFFLOAD", "scheme": "http", "hostname": host},
    ]
    with VolumeFileSystem(client=dummy_client) as fs:
        assert sync(fs.loop, fs._get_endpoints) == [
            _Endpoint(_FILES_API_V3, dummy_api, False),
            _Endpoint(_IO_OFFLOAD, dummy_api, False),
        ]

    dummy.endpoints = [{"api": "UNKNOWN", "scheme": "http", "hostname": host}]
    with VolumeFileSystem(client=dummy_client) as fs:
        assert sync(fs.loop, fs._get_endpoints) == _defaults(dummy_api)


_https = "https://example.cloud.databricks.com"
_http = "http://127.0.0.1:8000"


@pytest.mark.parametrize(
    ("entry", "workspace", "expected"),
    [
        (
            {
                "api": "IO_OFFLOAD",
                "scheme": "https",
                "hostname": "example.cloud.databricks.com",
            },
            _https,
            _Endpoint(_IO_OFFLOAD, _https, False),
        ),
        (
            {
                "api": "FILES_API",
                "api_version": "3.0",
                "scheme": "https",
                "hostname": "files.example.com:8443",
                "multipart_upload_supported": True,
            },
            _https,
            _Endpoint(_FILES_API_V3, "https://files.example.com:8443", True),
        ),
        # The storage proxy: Files API 2.0 over plain HTTP on another host.
        (
            {
                "api": "FILES_API",
                "api_version": "2.0",
                "scheme": "http",
                "hostname": "proxy",
            },
            _https,
            _Endpoint(_FILES_API_V2, "http://proxy", False),
        ),
        # Plain HTTP elsewhere carries the credentials in the clear.
        (
            {
                "api": "FILES_API",
                "api_version": "3.0",
                "scheme": "http",
                "hostname": "files",
            },
            _https,
            None,
        ),
        (
            {
                "api": "IO_OFFLOAD",
                "scheme": "http",
                "hostname": "example.cloud.databricks.com",
            },
            _https,
            None,
        ),
        # On a workspace reached by plain HTTP, its own host may use it.
        (
            {
                "api": "FILES_API",
                "api_version": "3.0",
                "scheme": "http",
                "hostname": "127.0.0.1:8000",
            },
            _http,
            _Endpoint(_FILES_API_V3, _http, False),
        ),
        # Unknown protocols and malformed addresses.
        (
            {
                "api": "FILES_API",
                "api_version": "4.0",
                "scheme": "https",
                "hostname": "h",
            },
            _https,
            None,
        ),
        (
            {
                "api": "FILES_API",
                "api_version": "2.0",
                "scheme": "ftp",
                "hostname": "h",
            },
            _https,
            None,
        ),
        (
            {
                "api": "FILES_API",
                "api_version": "2.0",
                "scheme": "https",
                "hostname": "h/p",
            },
            _https,
            None,
        ),
        (
            {
                "api": "FILES_API",
                "api_version": "2.0",
                "scheme": "https",
                "hostname": "u@h",
            },
            _https,
            None,
        ),
        (
            {
                "api": "FILES_API",
                "api_version": "2.0",
                "scheme": "https",
                "hostname": "h:x",
            },
            _https,
            None,
        ),
        ({"api": "FILES_API", "api_version": "2.0", "scheme": "https"}, _https, None),
        ({"api": ["IO_OFFLOAD"], "scheme": "https", "hostname": "h"}, _https, None),
        ("IO_OFFLOAD", _https, None),
    ],
)
def test_volume_parse_endpoint(entry, workspace, expected):
    assert _parse_endpoint(entry, workspace) == expected


@pytest.mark.parametrize(
    "kind", ["io_offload", "files_api_2", "storage_proxy", "files_api_3"]
)
def test_volume_open_on_endpoint(dummy_client, dummy, dummy_api, kind):
    """Files read and written in parts through the first endpoint listed."""
    port = dummy_api.rsplit(":", 1)[1]
    entry = {
        "io_offload": {"api": "IO_OFFLOAD", "hostname": f"127.0.0.1:{port}"},
        "files_api_2": {
            "api": "FILES_API",
            "api_version": "2.0",
            "hostname": f"127.0.0.1:{port}",
        },
        # Another host name of the dummy, as the storage proxy has one of its own.
        "storage_proxy": {
            "api": "FILES_API",
            "api_version": "2.0",
            "hostname": f"localhost:{port}",
        },
        "files_api_3": {
            "api": "FILES_API",
            "api_version": "3.0",
            "hostname": f"127.0.0.1:{port}",
        },
    }[kind]
    dummy.endpoints = [{**entry, "scheme": "http", "multipart_upload_supported": True}]
    file = "/Volumes/c/s/v/endpoint.bin"
    data = randbytes(20 * 1000 * 1000)

    with VolumeFileSystem(client=dummy_client) as fs:
        with fs.open(file, "wb") as f:
            f.write(data)
        assert dummy.volume_files[file].data == data
        with fs.open(file, "rb") as f:
            assert f.read() == data

    presigned = dummy.requests_to("/api/2.0/fs/create-")
    v3 = dummy.requests_to("/api/3.0/fs/files")
    if kind == "io_offload":
        assert {r.path for r in presigned} == {
            "/api/2.0/fs/create-upload-part-urls",
            "/api/2.0/fs/create-download-url",
        }
    else:
        assert presigned == []
    if kind == "files_api_3":
        actions = [r.params.get("action") for r in v3 if r.method == "POST"]
        assert actions == ["initiate-upload", "complete-upload"]
        assert all("upload_id" in r.params for r in v3 if r.method == "PUT")
    else:
        assert v3 == []


def test_volume_write_endpoint_takes_parts(dummy_client, dummy, dummy_api):
    """Reads take the first endpoint; writes the first taking multipart uploads."""
    host = dummy_api.removeprefix("http://")
    dummy.endpoints = [
        {"api": "FILES_API", "api_version": "2.0", "scheme": "http", "hostname": host},
        {
            "api": "IO_OFFLOAD",
            "scheme": "http",
            "hostname": host,
            "multipart_upload_supported": True,
        },
    ]
    file = "/Volumes/c/s/v/endpoint.bin"
    data = randbytes(20 * 1000 * 1000)

    with VolumeFileSystem(client=dummy_client) as fs:
        with fs.open(file, "wb") as f:
            f.write(data)
        with fs.open(file, "rb") as f:
            assert f.read() == data

    # Parts through presigned URLs, ranges from the Files API itself.
    assert dummy.requests_to("/api/2.0/fs/create-upload-part-urls")
    assert dummy.requests_to("/api/2.0/fs/create-download-url") == []


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


def test_volume_endpoints(client: WorkspaceClient):
    """The workspace lists endpoints, or the SDK's defaults are taken: either way, one to read and write by."""
    with VolumeFileSystem(client=client) as fs:
        endpoints = sync(fs.loop, fs._get_endpoints)
        assert endpoints
        assert all(e.base_url.startswith(("https://", "http://")) for e in endpoints)


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

    # Test copying it into a missing directory: the profile says the upload, multipart too, creates the parents.
    fs.cp_file(
        dbfs_url(f"{test_dir}/3_large.bin"),
        dbfs_url(f"{test_dir}/5_new/nested/3_large_copied.bin"),
    )
    assert bytes_sig(
        volume_download(client, f"{test_dir}/5_new/nested/3_large_copied.bin")
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

    items = sorted(
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(test_dir)
    )
    assert items == [
        (f"{test_dir}/directory", True),
        (f"{test_dir}/directory_copied", True),
    ]

    items = sorted(
        (i.path.rstrip("/"), i.is_directory)
        for i in client.files.list_directory_contents(f"{test_dir}/directory_copied")
    )
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
    one_part = randbytes(10 * 1024 * 1023)
    large_data = randbytes(12 * 1024 * 1024)

    # A file of one part is uploaded whole: just under 10 MiB is one, as its rest would
    # be smaller than the smallest part, 5 MiB. A larger one goes in parts.
    for data, in_parts in [(small_data, False), (one_part, False), (large_data, True)]:
        stream = BytesIO(data)
        with fs.open(dbfs_url(f"{test_dir}/1_data.bin"), mode="wb") as f:
            while chunk := stream.read(1234 * 1000):
                f.write(chunk)

        written = volume_download(client, f"{test_dir}/1_data.bin")
        assert in_parts == (f._upload is not None)
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

    with patch.object(VolumeReadableFile, "retry_delay", 0):
        with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
            assert f.read() == data

    assert not dummy.read_errors


def test_volume_file_read_retries_exhausted(
    dummy, dummy_api, client_event_loop, aiohttp_session
):
    data = randbytes(1000)
    dummy.vol_write(path, data)
    dummy.read_errors.extend([500] * 10)

    with patch.object(VolumeReadableFile, "retry_delay", 0):
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
    assert attempts == VolumeReadableFile.max_retries + 1


@pytest.mark.parametrize("status", [400, 403, 404])
def test_volume_file_read_no_retry(
    dummy, dummy_api, client_event_loop, aiohttp_session, status
):
    data = randbytes(1000)
    dummy.vol_write(path, data)
    dummy.read_errors.extend([status] * 2)

    with patch.object(VolumeReadableFile, "retry_delay", 0):
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


@pytest.mark.parametrize("use_presigned_url", [True, False])
@pytest.mark.parametrize("size", [1000, 20 * 1000 * 1000])
def test_volume_file_write_retries(
    dummy, dummy_api, client_event_loop, aiohttp_session, size, use_presigned_url
):
    """A whole file, or a part, is sent again after transient errors."""
    data = randbytes(size)
    dummy.upload_errors.extend([503, 500])

    with patch.object(VolumeWritableFile, "retry_delay", 0):
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            use_presigned_url=use_presigned_url,
        ) as f:
            f.write(data)

    assert dummy.volume_files[path].data == data
    assert not dummy.upload_errors


def test_volume_file_write_no_retry(
    dummy, dummy_api, client_event_loop, aiohttp_session
):
    dummy.upload_errors.extend([403, 403])

    with patch.object(VolumeWritableFile, "retry_delay", 0):
        with pytest.raises(PermissionError):
            with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
                f.write(b"data")

    assert len(dummy.upload_errors) == 1


@pytest.mark.parametrize("prefer_presigned_url", [True, False])
def test_volume_file_read_changed_file(
    dummy, dummy_api, client_event_loop, aiohttp_session, prefer_presigned_url
):
    """A file replaced while read fails, rather than mix two versions."""
    data = randbytes(20 * 1000 * 1000)
    dummy.vol_write(path, data)

    with _reader(
        aiohttp_session,
        dummy_api,
        client_event_loop,
        len(data),
        prefer_presigned_url=prefer_presigned_url,
    ) as f:
        assert f.read(100) == data[:100]
        # A newer version, by more than the second of HTTP dates.
        dummy.volume_files[path].last_modified += 2000
        f.seek(15 * 1000 * 1000)
        with pytest.raises(OSError, match="changed while it was read"):
            f.read(100)

    ranges = dummy.requests_to("/api/2.0/fs/files", "GET")
    assert "if-unmodified-since" in ranges[-1].headers


def test_volume_file_read_range_ignored(
    dummy, dummy_api, client_event_loop, aiohttp_session
):
    """A whole file answered to a range is refused, not taken for the range."""
    data = randbytes(20 * 1000 * 1000)
    dummy.vol_write(path, data)
    dummy.ranges_ignored = True

    with _reader(aiohttp_session, dummy_api, client_event_loop, len(data)) as f:
        with pytest.raises(OSError, match="answered with status 200"):
            f.read(100)


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


def test_volume_file_parts_write(dummy_api, dummy, client_event_loop, aiohttp_session):
    data = randbytes(20 * 1000 * 1000)

    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        # Several writes, so that the parts grow
        for i in range(0, len(data), 1000 * 1000):
            f.write(data[i : i + 1000 * 1000])
        assert not f.sequential

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


@pytest.mark.parametrize("use_presigned_url", [True, False])
@pytest.mark.parametrize("upload_mode", ["resumable", "unknown"])
def test_volume_file_unexpected_initiate_reply(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    use_presigned_url,
    upload_mode,
):
    # Resumable uploads too, which the SDK no longer speaks since 0.163.
    dummy.upload_mode = upload_mode

    with pytest.raises(OSError, match="Unexpected initiate-upload response"):
        with _writer(
            aiohttp_session,
            dummy_api,
            client_event_loop,
            use_presigned_url=use_presigned_url,
        ) as f:
            f.write(randbytes(20 * 1000 * 1000))

    assert path not in dummy.volume_files


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


@pytest.mark.parametrize("use_presigned_url", [True, False])
def test_volume_file_multipart_write_abort(
    dummy_api, dummy, client_event_loop, aiohttp_session, use_presigned_url
):
    # Refused abort URLs change nothing: the abort is a POST to the Files API.
    dummy.abort_supported = False

    with patch.object(
        VolumeWritableFile,
        "_upload_part",
        AsyncMock(side_effect=RuntimeError("Dummy")),
    ):
        with pytest.raises(OSError) as exc_info:
            with _writer(
                aiohttp_session,
                dummy_api,
                client_event_loop,
                use_presigned_url=use_presigned_url,
            ) as f:
                f.write(randbytes(20 * 1000 * 1000))

    # The upload's failure is raised, not the abort's
    assert "Failed to upload a part" in str(exc_info.value)
    assert path not in dummy.volume_files
    assert dummy.multipart_uploads == {}
    aborts = dummy.requests_to("/api/2.0/fs/files", "POST")
    assert [r.params["action"] for r in aborts][-1] == "abort-upload"
    assert dummy.requests_to("/api/2.0/fs/create-abort-upload-url") == []


def test_volume_file_abort_failure_warns(
    dummy_api, dummy, client_event_loop, aiohttp_session, caplog
):
    """A failed abort is logged, and the upload's own failure raised."""

    def fail(*args, **kwargs):
        # The server forgets the upload, so that its abort fails.
        dummy.multipart_uploads.clear()
        raise RuntimeError("Dummy")

    with patch.object(VolumeWritableFile, "_upload_part", AsyncMock(side_effect=fail)):
        with pytest.raises(OSError, match="Failed to upload a part"):
            with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
                f.write(randbytes(20 * 1000 * 1000))

    assert any(
        r.levelname == "WARNING" and "Failed to abort upload" in r.getMessage()
        for r in caplog.records
    )


def test_volume_file_write_parquet(
    client_event_loop,
    dummy_api,
    dummy,
    dummy_table,
    aiohttp_session,
):
    with _writer(aiohttp_session, dummy_api, client_event_loop) as f:
        pq.write_table(pa.Table.from_pylist(dummy_table), f)

    df = pd.read_parquet(BytesIO(dummy.volume_files[path].data))

    df.sort_values(by="id")
    assert df.shape == (256 * 1024, 3)
    assert df.head(1).to_dict("records") == [dummy_table[0]]
    assert df.iloc[123:456, :].to_dict("records") == dummy_table[123:456]
    assert df.tail(1).to_dict("records") == [dummy_table[-1]]


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
@pytest.mark.parametrize("commit", [True, False])
def test_volume_file_transaction(
    dummy_api,
    dummy,
    client_event_loop,
    aiohttp_session,
    size,
    commit,
):
    """In a transaction, nothing is visible before commit, and discard leaves nothing."""
    data = randbytes(size)
    f = TransactionFile(
        AdaptiveWritableTransaction(
            _writer(aiohttp_session, dummy_api, client_event_loop)
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
