"""Tests of the dummy server itself, through the Databricks SDK's own client.

They pin the dummy to what the SDK sends and expects, apart from this package's code. Behaviour that the REST
reference leaves open is marked as an assumption, as in `tests/dummy_api.py`.
"""

import base64
from io import BytesIO
from random import randbytes

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    AlreadyExists,
    BadRequest,
    DatabricksError,
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectType,
)

NOTEBOOK = b"# Databricks notebook source\nprint(1)"


@pytest.fixture
def w(dummy_client, dummy):
    """The SDK's client of a dummy server with empty storage."""
    return dummy_client


@pytest.fixture
def parts_client(dummy_api, dummy):
    """A client of its own, uploading in parts from 1 MiB, so that the tests send little."""
    client = WorkspaceClient(host=dummy_api, token="dummy", auth_type="pat")
    client.files._config.files_ext_multipart_upload_min_stream_size = _MiB

    def no_single_shot(*args, **kwargs):
        raise AssertionError("A single-shot upload, as a fallback")

    # The SDK falls back to a single-shot upload when parts fail: the tests must see them work.
    client.files._single_thread_single_shot_upload = no_single_shot
    return client


### Files API

_MiB = 1024 * 1024
V = "/Volumes/c/s/v"


def test_files_upload_download_delete(w):
    data = randbytes(1000)
    w.files.upload(f"{V}/a.bin", BytesIO(data), overwrite=True)
    with w.files.download(f"{V}/a.bin").contents as f:
        assert f.read() == data

    metadata = w.files.get_metadata(f"{V}/a.bin")
    assert metadata.content_length == len(data)
    assert metadata.content_type == "application/octet-stream"
    assert metadata.last_modified.endswith("GMT")

    # The reference: an existing file is overwritten unless overwrite is false.
    w.files.upload(f"{V}/a.bin", BytesIO(b"x"))
    with w.files.download(f"{V}/a.bin").contents as f:
        assert f.read() == b"x"
    with pytest.raises(AlreadyExists):
        w.files.upload(f"{V}/a.bin", BytesIO(b"y"), overwrite=False)

    w.files.delete(f"{V}/a.bin")
    with pytest.raises(NotFound):
        w.files.download(f"{V}/a.bin").contents.read()
    with pytest.raises(NotFound):
        w.files.get_metadata(f"{V}/a.bin")
    with pytest.raises(NotFound):
        w.files.delete(f"{V}/a.bin")
    # Assumed: the error of a path outside the volumes. A HEAD response has no body to name it.
    with pytest.raises(InvalidParameterValue):
        w.files.delete("/Volumes/c/s")
    with pytest.raises(BadRequest):
        w.files.get_metadata("/Volumes/c/s")


def test_files_download_range(w, dummy):
    data = randbytes(1000)
    dummy.vol_write(f"{V}/r.bin", data)

    def get(range_):
        return w.api_client.do(
            "GET",
            f"/api/2.0/fs/files{V}/r.bin",
            headers={"Range": range_},
            response_headers=["content-range"],
            raw=True,
        )

    response = get("bytes=100-199")
    assert response["content-range"] == "bytes 100-199/1000"
    assert response["contents"].read() == data[100:200]
    # The SDK resumes downloads with open ranges; suffixes are HTTP's too.
    assert get("bytes=900-")["contents"].read() == data[900:]
    assert get("bytes=-10")["contents"].read() == data[-10:]
    assert get("bytes=990-2000")["contents"].read() == data[990:]
    with pytest.raises(DatabricksError):
        get("bytes=1000-")


def test_files_presigned_download(w, dummy):
    """The SDK downloads by a presigned URL first, which works when the Files API itself refuses."""
    data = randbytes(1000)
    dummy.vol_write(f"{V}/p.bin", data)
    dummy.presigned_urls_only = True
    with w.files.download(f"{V}/p.bin").contents as f:
        assert f.read() == data

    with pytest.raises(PermissionDenied):
        w.api_client.do("GET", f"/api/2.0/fs/files{V}/p.bin", raw=True)


def test_files_directories(w):
    w.files.create_directory(f"{V}/a/b")
    # The reference: creating an existing directory succeeds.
    w.files.create_directory(f"{V}/a/b")
    w.files.get_directory_metadata(f"{V}/a")
    w.files.get_directory_metadata(V)
    # Assumed: an upload creates the missing parents.
    w.files.upload(f"{V}/a/c/f.bin", BytesIO(b"12345"))

    entries = list(w.files.list_directory_contents(f"{V}/a"))
    assert [(e.path, e.name, e.is_directory) for e in entries] == [
        (f"{V}/a/b", "b", True),
        (f"{V}/a/c", "c", True),
    ]
    (entry,) = w.files.list_directory_contents(f"{V}/a/c")
    assert (entry.path, entry.is_directory, entry.file_size) == (
        f"{V}/a/c/f.bin",
        False,
        5,
    )
    assert entry.last_modified > 0
    assert list(w.files.list_directory_contents(f"{V}/a/b")) == []

    with pytest.raises(NotFound):
        w.files.get_directory_metadata(f"{V}/missing")
    with pytest.raises(NotFound):
        list(w.files.list_directory_contents(f"{V}/missing"))
    # Assumed: a file is no directory, and a directory no file.
    with pytest.raises(NotFound):
        w.files.get_directory_metadata(f"{V}/a/c/f.bin")
    with pytest.raises(NotFound):
        list(w.files.list_directory_contents(f"{V}/a/c/f.bin"))
    with pytest.raises(NotFound):
        w.files.get_metadata(f"{V}/a/c")
    with pytest.raises(NotFound):
        w.files.delete(f"{V}/a/c")
    # Assumed: a file in the way fails as an existing file does.
    with pytest.raises(AlreadyExists):
        w.files.create_directory(f"{V}/a/c/f.bin/d")
    with pytest.raises(AlreadyExists):
        w.files.upload(f"{V}/a/c", BytesIO(b"1"))


def test_files_delete_directory(w):
    w.files.create_directory(f"{V}/d/sub")

    # The reference: only an empty directory is deleted.
    with pytest.raises(BadRequest) as e:
        w.files.delete_directory(f"{V}/d")
    assert "is not empty" in str(e.value)

    w.files.delete_directory(f"{V}/d/sub")
    w.files.delete_directory(f"{V}/d")
    with pytest.raises(NotFound):
        w.files.get_directory_metadata(f"{V}/d")
    with pytest.raises(NotFound):
        w.files.delete_directory(f"{V}/d")


def test_files_list_pages(w, dummy):
    names = [f"f{i:02}" for i in range(7)]
    for n in names:
        dummy.vol_write(f"{V}/l/{n}", b"")

    # The SDK follows next_page_token.
    entries = w.files.list_directory_contents(f"{V}/l", page_size=3)
    assert [e.name for e in entries] == names

    page = w.files._api.do(
        "GET", f"/api/2.0/fs/directories{V}/l", query={"page_size": 3}
    )
    assert len(page["contents"]) == 3
    assert "next_page_token" in page
    with pytest.raises(InvalidParameterValue):
        w.files._api.do(
            "GET", f"/api/2.0/fs/directories{V}/l", query={"page_token": "?"}
        )


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_files_upload_in_parts(parts_client, dummy, upload_mode):
    """The SDK uploads in parts through presigned URLs, by the API that the server chooses."""
    dummy.upload_mode = upload_mode
    data = randbytes(3 * _MiB + 1000)

    parts_client.files.upload(
        f"{V}/m.bin", BytesIO(data), part_size=_MiB, use_parallel=False
    )
    assert dummy.volume_files[f"{V}/m.bin"].data == data
    assert dummy.multipart_uploads == {}
    assert dummy.resumable_uploads == {}

    # The reference: an existing file without overwrite refuses the upload from its start.
    with pytest.raises(AlreadyExists):
        parts_client.files.upload(
            f"{V}/m.bin", BytesIO(data), part_size=_MiB, overwrite=False
        )


def test_files_upload_in_parts_parallel(parts_client, dummy):
    data = randbytes(5 * _MiB)
    parts_client.files.upload(f"{V}/p.bin", BytesIO(data), part_size=_MiB)
    assert dummy.volume_files[f"{V}/p.bin"].data == data


@pytest.mark.parametrize("upload_mode", ["multipart", "resumable"])
def test_files_upload_storage_proxy(parts_client, dummy_api, dummy, upload_mode):
    """Through the storage proxy, the parts go to the Files API itself, as fsspec-databricks sends them."""
    dummy.upload_mode = upload_mode
    dummy.storage_proxy_available = True
    parts_client.files._config.experimental_files_ext_enable_storage_proxy = True
    parts_client.files._STORAGE_PROXY_HOSTNAME = dummy_api
    data = randbytes(3 * _MiB + 1000)

    parts_client.files.upload(
        f"{V}/s.bin", BytesIO(data), part_size=_MiB, use_parallel=False
    )
    assert parts_client.files._dp_hostname_available is True
    with parts_client.files.download(f"{V}/s.bin").contents as f:
        assert f.read() == data


def test_files_upload_sessions(w, dummy):
    """The multipart upload API's requests, as the SDK and fsspec-databricks send them."""
    do = w.api_client.do
    url = f"/api/2.0/fs/files{V}/u.bin"

    token = do("POST", url, query={"action": "initiate-upload"})["multipart_upload"][
        "session_token"
    ]
    (part,) = do(
        "POST",
        "/api/2.0/fs/create-upload-part-urls",
        body={
            "path": f"{V}/u.bin",
            "session_token": token,
            "start_part_number": 1,
            "count": 1,
            "expire_time": "2099-01-01T00:00:00Z",
        },
    )["upload_part_urls"]
    assert part["part_number"] == 1

    # Not completed with a wrong ETag; aborted by a POST, as the SDK does.
    with pytest.raises(InvalidParameterValue):
        do(
            "POST",
            url,
            query={
                "action": "complete-upload",
                "upload_type": "multipart",
                "session_token": token,
            },
            body={"parts": [{"part_number": 1, "etag": "wrong"}]},
        )
    do(
        "POST",
        url,
        query={
            "action": "abort-upload",
            "upload_type": "multipart",
            "session_token": token,
        },
    )
    assert dummy.multipart_uploads == {}
    assert f"{V}/u.bin" not in dummy.volume_files

    # Assumed: an unknown session is not found.
    with pytest.raises(NotFound):
        do(
            "POST",
            "/api/2.0/fs/create-resumable-upload-url",
            body={"path": f"{V}/u.bin", "session_token": token},
        )

    # Azure Databricks refuses to abort by a presigned URL.
    dummy.abort_supported = False
    with pytest.raises(BadRequest):
        do(
            "POST",
            "/api/2.0/fs/create-abort-upload-url",
            body={"path": f"{V}/u.bin", "session_token": token, "expire_time": "x"},
        )


### Workspace API


def test_workspace_mkdirs_get_status_list(w):
    w.workspace.mkdirs("/a/b")
    # The reference: an existing directory makes mkdirs succeed and do nothing.
    w.workspace.mkdirs("/a/b")

    status = w.workspace.get_status("/a/b")
    assert status.object_type == ObjectType.DIRECTORY
    assert status.object_id is not None
    assert status.resource_id is not None
    assert status.created_at is None and status.size is None

    w.workspace.upload("/a/f.bin", b"12345", format=ImportFormat.RAW)
    status = w.workspace.get_status("/a/f.bin")
    assert (status.object_type, status.size, status.language) == (
        ObjectType.FILE,
        5,
        None,
    )
    assert status.created_at == status.modified_at

    # In creation order.
    assert [i.path for i in w.workspace.list("/a")] == ["/a/b", "/a/f.bin"]
    assert list(w.workspace.list("/a/b")) == []
    # Assumed: listing a file gives the file, as the DBFS API does.
    assert [i.path for i in w.workspace.list("/a/f.bin")] == ["/a/f.bin"]

    with pytest.raises(ResourceDoesNotExist):
        w.workspace.get_status("/missing")
    with pytest.raises(ResourceDoesNotExist):
        list(w.workspace.list("/missing"))
    # The reference: an object at a prefix of the path fails mkdirs.
    with pytest.raises(ResourceAlreadyExists):
        w.workspace.mkdirs("/a/f.bin/c")
    with pytest.raises(InvalidParameterValue):
        w.workspace.get_status("relative")


def test_workspace_import_export(w):
    w.workspace.mkdirs("/d")
    data = randbytes(1000)

    w.workspace.upload("/d/raw.bin", data, format=ImportFormat.RAW)
    with w.workspace.download("/d/raw.bin", format=ExportFormat.AUTO) as f:
        assert f.read() == data
    # The base API's JSON export, in base64.
    assert base64.b64decode(w.workspace.export("/d/raw.bin").content) == data

    w.workspace.upload(
        "/d/nb", NOTEBOOK, format=ImportFormat.SOURCE, language=Language.PYTHON
    )
    status = w.workspace.get_status("/d/nb")
    assert (status.object_type, status.language, status.size) == (
        ObjectType.NOTEBOOK,
        Language.PYTHON,
        None,
    )
    with w.workspace.download("/d/nb", format=ExportFormat.SOURCE) as f:
        assert f.read() == NOTEBOOK
    assert w.workspace.export("/d/nb").file_type == "py"

    # The base API's JSON import.
    w.workspace.import_(
        "/d/json.bin",
        content=base64.b64encode(b"json").decode(),
        format=ImportFormat.RAW,
    )
    with w.workspace.download("/d/json.bin") as f:
        assert f.read() == b"json"


def test_workspace_import_auto(w):
    """The reference: AUTO imports a notebook by its extension and header, and removes the extension."""
    w.workspace.mkdirs("/d")
    w.workspace.upload("/d/nb.py", NOTEBOOK, format=ImportFormat.AUTO)
    w.workspace.upload("/d/plain.py", b"print(1)", format=ImportFormat.AUTO)

    assert sorted(
        (i.path, i.object_type, i.language) for i in w.workspace.list("/d")
    ) == [
        ("/d/nb", ObjectType.NOTEBOOK, Language.PYTHON),
        ("/d/plain.py", ObjectType.FILE, None),
    ]


def test_workspace_import_errors(w):
    w.workspace.mkdirs("/d")
    w.workspace.upload("/d/f", b"1", format=ImportFormat.RAW)

    # The reference: an existing path without overwrite.
    with pytest.raises(ResourceAlreadyExists):
        w.workspace.upload("/d/f", b"2", format=ImportFormat.RAW)
    w.workspace.upload("/d/f", b"2", format=ImportFormat.RAW, overwrite=True)
    with w.workspace.download("/d/f") as f:
        assert f.read() == b"2"
    with pytest.raises(ResourceAlreadyExists):
        w.workspace.upload("/d", b"2", format=ImportFormat.RAW, overwrite=True)

    # Assumed: a missing parent folder is not created.
    with pytest.raises(ResourceDoesNotExist):
        w.workspace.upload("/missing/f", b"1", format=ImportFormat.RAW)
    # The reference: a single file imported as SOURCE needs its language.
    with pytest.raises(DatabricksError) as e:
        w.workspace.upload("/d/nb", NOTEBOOK, format=ImportFormat.SOURCE)
    assert e.value.error_code == "INVALID_PARAMETER_VALUE"
    # The reference: the content is limited to 10 MB.
    with pytest.raises(DatabricksError) as e:
        w.workspace.upload(
            "/d/big", bytes(10 * 1024 * 1024 + 1), format=ImportFormat.RAW
        )
    assert e.value.error_code == "MAX_NOTEBOOK_SIZE_EXCEEDED"
    with pytest.raises(ResourceDoesNotExist):
        w.workspace.download("/d/missing").read()


def test_workspace_delete(w):
    w.workspace.mkdirs("/d/sub")
    w.workspace.upload("/d/sub/f", b"1", format=ImportFormat.RAW)

    # The reference: a non-empty directory needs recursive.
    with pytest.raises(BadRequest) as e:
        w.workspace.delete("/d")
    assert e.value.error_code == "DIRECTORY_NOT_EMPTY"
    assert "is not empty" in str(e.value)

    w.workspace.delete("/d/sub/f")
    w.workspace.delete("/d", recursive=True)
    with pytest.raises(ResourceDoesNotExist):
        w.workspace.get_status("/d/sub")
    with pytest.raises(ResourceDoesNotExist):
        w.workspace.delete("/d")


### Legacy DBFS API


def test_dbfs_mkdirs_get_status_list(w):
    w.dbfs.mkdirs("/a/b")
    w.dbfs.put("/a/f.bin", contents=base64.b64encode(b"12345").decode())

    status = w.dbfs.get_status("/a/f.bin")
    assert (status.path, status.is_dir, status.file_size) == ("/a/f.bin", False, 5)
    assert w.dbfs.get_status("dbfs:/a/b").is_dir

    # The base API's list; the mixin's lists the same.
    assert [i.path for i in w.dbfs._dbfs_api.list("/a")] == ["/a/b", "/a/f.bin"]
    assert [i.path for i in w.dbfs.list("/a")] == ["/a/b", "/a/f.bin"]
    assert [i.path for i in w.dbfs.list("/a", recursive=True)] == ["/a/f.bin"]
    # The reference: listing a file gives its details.
    assert [i.path for i in w.dbfs._dbfs_api.list("/a/f.bin")] == ["/a/f.bin"]

    with pytest.raises(ResourceDoesNotExist):
        w.dbfs.get_status("/missing")
    with pytest.raises(ResourceDoesNotExist):
        list(w.dbfs._dbfs_api.list("/missing"))
    # The reference: a file at a prefix of the path fails mkdirs.
    with pytest.raises(ResourceAlreadyExists):
        w.dbfs.mkdirs("/a/f.bin/c")
    assert w.dbfs.exists("/a/b") and not w.dbfs.exists("/missing")


def test_dbfs_streams(w):
    data = randbytes(2_500_000)
    handle = w.dbfs.create("/s/f.bin").handle
    for i in range(0, len(data), 1024 * 1024):
        w.dbfs.add_block(handle, base64.b64encode(data[i : i + 1024 * 1024]).decode())
    w.dbfs.close(handle)
    assert w.dbfs.get_status("/s/f.bin").file_size == len(data)

    # The reference: blocks are limited to 1 MB, and unknown handles refused.
    handle = w.dbfs.create("/s/g.bin").handle
    with pytest.raises(DatabricksError) as e:
        w.dbfs.add_block(handle, base64.b64encode(bytes(1024 * 1024 + 1)).decode())
    assert e.value.error_code == "MAX_BLOCK_SIZE_EXCEEDED"
    w.dbfs.close(handle)
    with pytest.raises(ResourceDoesNotExist):
        w.dbfs.close(handle)
    with pytest.raises(ResourceDoesNotExist):
        w.dbfs.add_block(handle, "")

    # The reference: an existing file without overwrite.
    with pytest.raises(ResourceAlreadyExists):
        w.dbfs.create("/s/f.bin")
    w.dbfs.close(w.dbfs.create("/s/f.bin", overwrite=True).handle)
    assert w.dbfs.get_status("/s/f.bin").file_size == 0


def test_dbfs_read(w):
    data = randbytes(3000)
    w.dbfs.put("/r/f.bin", contents=base64.b64encode(data).decode())

    response = w.dbfs.read("/r/f.bin", offset=1000, length=500)
    assert response.bytes_read == 500
    assert base64.b64decode(response.data) == data[1000:1500]
    assert w.dbfs.read("/r/f.bin", offset=5000).bytes_read == 0

    # The reference: over 1 MB, a directory, or a negative offset.
    with pytest.raises(DatabricksError) as e:
        w.dbfs.read("/r/f.bin", length=1024 * 1024 + 1)
    assert e.value.error_code == "MAX_READ_SIZE_EXCEEDED"
    with pytest.raises(InvalidParameterValue):
        w.dbfs.read("/r")
    with pytest.raises(InvalidParameterValue):
        w.dbfs.read("/r/f.bin", offset=-1)
    with pytest.raises(ResourceDoesNotExist):
        w.dbfs.read("/r/missing")


def test_dbfs_put_limit(w):
    # The reference: contents passed inline are limited to 1 MB.
    with pytest.raises(DatabricksError) as e:
        w.dbfs.put("/p.bin", contents=base64.b64encode(bytes(1024 * 1024 + 1)).decode())
    assert e.value.error_code == "MAX_BLOCK_SIZE_EXCEEDED"
    w.dbfs.put("/p.bin", contents="")
    with pytest.raises(ResourceAlreadyExists):
        w.dbfs.put("/p.bin", contents="")


def test_dbfs_mixin_open_upload_download(w):
    data = randbytes(2_500_000)
    w.dbfs.upload("/m/f.bin", BytesIO(data))
    with w.dbfs.download("/m/f.bin") as f:
        assert f.read() == data
    with w.dbfs.open("/m/f.bin", read=True) as f:
        assert f.read(1000) == data[:1000]

    with w.dbfs.open("/m/g.bin", write=True) as f:
        f.write(data)
    with w.dbfs.download("/m/g.bin") as f:
        assert f.read() == data

    with pytest.raises(ResourceAlreadyExists):
        w.dbfs.upload("/m/f.bin", BytesIO(b"x"))
    w.dbfs.upload("/m/f.bin", BytesIO(b"x"), overwrite=True)
    with w.dbfs.download("/m/f.bin") as f:
        assert f.read() == b"x"

    w.dbfs.copy("/m/g.bin", "/m/copy.bin")
    assert w.dbfs.get_status("/m/copy.bin").file_size == len(data)


def test_dbfs_move(w):
    w.dbfs.mkdirs("/v/dir/sub")
    w.dbfs.put("/v/dir/sub/f", contents=base64.b64encode(b"f").decode())
    w.dbfs.put("/v/g", contents="")

    # The reference: a directory moves with all in it.
    w.dbfs.move_("/v/dir", "/v/moved")
    assert [i.path for i in w.dbfs.list("/v/moved", recursive=True)] == [
        "/v/moved/sub/f"
    ]
    assert not w.dbfs.exists("/v/dir")

    # The reference: an existing destination, or a missing source.
    with pytest.raises(ResourceAlreadyExists):
        w.dbfs.move("/v/g", "/v/moved")
    with pytest.raises(ResourceDoesNotExist):
        w.dbfs.move("/v/missing", "/v/other")


def test_dbfs_delete(w):
    w.dbfs.mkdirs("/x/dir/sub")
    w.dbfs.put("/x/dir/f", contents="")

    # The reference: IO_ERROR for a non-empty directory without recursive.
    with pytest.raises(DatabricksError) as e:
        w.dbfs._dbfs_api.delete("/x/dir")
    assert e.value.error_code == "IO_ERROR"
    # The mixin refuses directories itself.
    with pytest.raises(IOError):
        w.dbfs.delete("/x/dir")

    w.dbfs._dbfs_api.delete("/x/dir/sub")
    w.dbfs.delete("/x/dir", recursive=True)
    assert not w.dbfs.exists("/x/dir")
    # Assumed: a missing path is no error, as the reference names none.
    w.dbfs._dbfs_api.delete("/x/dir", recursive=True)


def test_dbfs_disabled(w, dummy):
    """A workspace disabling the DBFS root refuses the API, as real workspaces answer."""
    setting = w.workspace_settings_v2.get_public_workspace_setting(
        "disable_legacy_dbfs"
    )
    assert setting.effective_boolean_val.value is False

    dummy.legacy_dbfs_disabled = True
    setting = w.workspace_settings_v2.get_public_workspace_setting(
        "disable_legacy_dbfs"
    )
    assert setting.effective_boolean_val.value is True
    with pytest.raises(PermissionDenied):
        w.dbfs.get_status("/")


### Unity Catalog API


def test_unity_catalog(w, dummy):
    w.files.create_directory(f"{V}/d")
    w.files.upload("/Volumes/c/s2/v2/f.bin", BytesIO(b"f"))

    assert [c.name for c in w.catalogs.list()] == ["c"]
    assert [s.full_name for s in w.schemas.list("c")] == ["c.s", "c.s2"]
    assert [v.full_name for v in w.volumes.list("c", "s")] == ["c.s.v"]
    # Assumed: every catalog, schema and volume exists.
    assert w.catalogs.get("other").name == "other"
    assert w.schemas.get("c.other").catalog_name == "c"
    assert w.volumes.read("c.s.other").schema_name == "s"


### Recorded requests


def test_requests_recorded(w, dummy):
    w.workspace.mkdirs("/a")
    w.files.upload(f"{V}/r.bin", BytesIO(b"data"), overwrite=True)
    w.dbfs.mkdirs("/x")

    assert [tuple(r) for r in dummy.requests_to("/api/2.0/workspace/")] == [
        ("POST", "/api/2.0/workspace/mkdirs")
    ]
    put = dummy.requests_to("/api/2.0/fs/files", "PUT")
    assert [(r.path, r.params) for r in put] == [
        (f"/api/2.0/fs/files{V}/r.bin", {"overwrite": "true"})
    ]
    assert dummy.requests_to("/api/2.0/dbfs/")[0].params == {"path": "/x"}

    dummy.reset()
    assert dummy.requests == []
