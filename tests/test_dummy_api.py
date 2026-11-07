"""Tests of the dummy server itself, through the Databricks SDK's own client.

They pin the dummy to what the SDK sends and expects, apart from this package's code. Behaviour that the REST
reference leaves open is marked as an assumption, as in `tests/dummy_api.py`.
"""

import base64
from io import BytesIO
from random import randbytes

import pytest
from databricks.sdk.errors import (
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
def w(dummy_client, dummy_storage_context, dummy_api_context):
    """The SDK's client of a dummy server with empty storage."""
    return dummy_client


### Files API


def test_files_upload_download_delete(w):
    data = randbytes(1000)
    w.files.upload("/Volumes/c/s/v/a.bin", BytesIO(data), overwrite=True)
    assert w.files.download("/Volumes/c/s/v/a.bin").contents.read() == data
    w.files.delete("/Volumes/c/s/v/a.bin")
    with pytest.raises(NotFound):
        w.files.download("/Volumes/c/s/v/a.bin").contents.read()


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


def test_dbfs_disabled(w, dummy_storage_context):
    """A workspace disabling the DBFS root refuses the API, as real workspaces answer."""
    setting = w.workspace_settings_v2.get_public_workspace_setting(
        "disable_legacy_dbfs"
    )
    assert setting.effective_boolean_val.value is False

    dummy_storage_context.legacy_dbfs_disabled = True
    setting = w.workspace_settings_v2.get_public_workspace_setting(
        "disable_legacy_dbfs"
    )
    assert setting.effective_boolean_val.value is True
    with pytest.raises(PermissionDenied):
        w.dbfs.get_status("/")
