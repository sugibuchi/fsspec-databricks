"""Offline tests of `DBFS`, on a fake DBFS API."""

import base64
import errno
import posixpath
from random import randbytes
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import (
    DatabricksError,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.sdk.service.files import FileInfo
from fsspec.asyn import sync

from fsspec_databricks.dbfs import DBFS


class FakeDbfsAPI:
    """An in-memory `WorkspaceClient.dbfs`, answering as the DBFS API does, recording its calls."""

    def __init__(self):
        self.dirs = {"/"}
        self.data: dict[str, bytes] = {}
        self.calls: list[tuple] = []
        self.handles: dict[int, str] = {}
        self.fail_add_block = False
        self._api = SimpleNamespace(
            _cfg=SimpleNamespace(workspace_id=None), do=self._do
        )

    def _exists(self, path):
        return path in self.dirs or path in self.data

    def _children(self, path):
        return sorted(
            p
            for p in (*self.dirs, *self.data)
            if p != "/" and posixpath.dirname(p) == path
        )

    def _info(self, path):
        if path in self.dirs:
            return FileInfo(path=path, is_dir=True, file_size=0)
        if path in self.data:
            return FileInfo(path=path, is_dir=False, file_size=len(self.data[path]))
        raise ResourceDoesNotExist(f"No file or directory exists on path {path}.")

    def _put(self, path, data):
        self.mkdirs(posixpath.dirname(path))
        self.data[path] = data

    def get_status(self, path):
        self.calls.append(("get_status", path))
        return self._info(path)

    def list(self, path):
        self.calls.append(("list", path))
        info = self._info(path)
        if not info.is_dir:
            return iter([info])
        return iter([self._info(p) for p in self._children(path)])

    def mkdirs(self, path):
        self.calls.append(("mkdirs", path))
        while path not in self.dirs:
            if path in self.data:
                raise ResourceAlreadyExists(f"{path} is a file")
            self.dirs.add(path)
            path = posixpath.dirname(path)

    def move(self, source_path, destination_path):
        self.calls.append(("move", source_path, destination_path))
        self._info(source_path)
        if self._exists(destination_path):
            raise ResourceAlreadyExists(f"{destination_path} already exists")
        for p in [
            p for p in self.dirs if p == source_path or p.startswith(source_path + "/")
        ]:
            self.dirs.remove(p)
            self.dirs.add(destination_path + p[len(source_path) :])
        for p in [
            p for p in self.data if p == source_path or p.startswith(source_path + "/")
        ]:
            self.data[destination_path + p[len(source_path) :]] = self.data.pop(p)

    def copy(self, src, dst, *, recursive=False, overwrite=False):
        self.calls.append(("copy", src, dst))
        self._put(dst, self.data[src])

    def _create(self, path, overwrite):
        if path in self.dirs or (path in self.data and not overwrite):
            raise ResourceAlreadyExists(f"{path} already exists")
        self._put(path, b"")

    def read(self, path, *, offset=0, length=512 * 1024):
        self.calls.append(("read", path, offset, length))
        if length > 1024 * 1024:
            raise DatabricksError("Too long", error_code="MAX_READ_SIZE_EXCEEDED")
        data = self.data[path][offset : offset + length]
        return SimpleNamespace(bytes_read=len(data), data=base64.b64encode(data))

    def put(self, path, *, contents=None, overwrite=False):
        data = base64.b64decode(contents or "")
        self.calls.append(("put", path, len(data), overwrite))
        if len(data) > 1024 * 1024:
            raise DatabricksError("Too big", error_code="MAX_BLOCK_SIZE_EXCEEDED")
        self._create(path, overwrite)
        self._put(path, data)

    def create(self, path, *, overwrite=False):
        self.calls.append(("create", path, overwrite))
        # As on DBFS: the file exists, empty, from its creation on.
        self._create(path, overwrite)
        self.handles[len(self.handles) + 1] = path
        return SimpleNamespace(handle=len(self.handles))

    def add_block(self, handle, data):
        block = base64.b64decode(data)
        self.calls.append(("add_block", handle, len(block)))
        if len(block) > 1024 * 1024:
            raise DatabricksError("Too big", error_code="MAX_BLOCK_SIZE_EXCEEDED")
        if self.fail_add_block:
            raise DatabricksError("Failed", error_code="INTERNAL_ERROR")
        self.data[self.handles[handle]] += block

    def close(self, handle):
        self.calls.append(("close", handle))
        if self.handles.pop(handle, None) is None:
            raise ResourceDoesNotExist(f"Unknown handle {handle}")

    def _do(self, method, url, body=None, headers=None):
        assert url == "/api/2.0/dbfs/delete"
        path, recursive = body["path"], body.get("recursive", False)
        self.calls.append(("delete", path, recursive))
        self._info(path)
        if self._children(path) and not recursive:
            raise DatabricksError("Directory is not empty", error_code="IO_ERROR")
        self.dirs = {
            p for p in self.dirs if not (p == path or p.startswith(path + "/"))
        }
        self.data = {
            p: d
            for p, d in self.data.items()
            if not p.startswith(path + "/") and p != path
        }

    def calls_of(self, name):
        return [c for c in self.calls if c[0] == name]


@pytest.fixture
def api():
    api = FakeDbfsAPI()
    api.mkdirs("/t/dir/sub")
    api.mkdirs("/t/empty")
    api._put("/t/data.bin", b"data")
    api._put("/t/dir/a.bin", b"a")
    api._put("/t/dir/sub/b.bin", b"b")
    api.calls.clear()
    return api


@pytest.fixture
def fs(api):
    client = MagicMock()
    client.dbfs = api
    with DBFS(client=client) as fs:
        yield fs


def test_dbfs_info_and_ls(fs):
    assert fs.info("dbfs:/t/data.bin")["size"] == 4
    assert fs.info("/")["name"] == "/"
    assert fs.ls("/t", detail=False) == ["/t/data.bin", "/t/dir", "/t/empty"]
    with pytest.raises(FileNotFoundError):
        fs.info("/t/missing")


def test_dbfs_rmdir_refuses_non_empty(fs, api):
    """rmdir deletes only an empty directory, never recursively."""
    with pytest.raises(OSError) as e:
        fs.rmdir("/t/dir")
    assert e.value.errno == errno.ENOTEMPTY
    assert fs.exists("/t/dir/a.bin")
    assert api.calls_of("delete") == []

    with pytest.raises(NotADirectoryError):
        fs.rmdir("/t/data.bin")
    with pytest.raises(FileNotFoundError):
        fs.rmdir("/t/missing")

    fs.rmdir("/t/empty")
    assert api.calls_of("delete") == [("delete", "/t/empty", False)]
    assert not fs.exists("/t/empty")


def test_dbfs_rmdir_refused_by_api(fs, api):
    """A directory filled after it was listed is refused by the API's non-recursive delete."""
    original = api.list
    api.list = lambda path: iter([]) if path == "/t/dir" else original(path)

    with pytest.raises(OSError) as e:
        fs.rmdir("/t/dir")
    assert e.value.errno == errno.ENOTEMPTY
    assert fs.exists("/t/dir/a.bin")


def test_dbfs_rm(fs, api):
    with pytest.raises(IsADirectoryError):
        fs.rm("/t/dir")
    fs.rm("/t/data.bin")
    assert not fs.exists("/t/data.bin")

    fs.rm("/t/dir", recursive=True)
    assert api.calls_of("delete")[-1] == ("delete", "/t/dir", True)
    assert not fs.exists("/t/dir")

    with pytest.raises(FileNotFoundError):
        fs.rm("/t/dir", recursive=True)


def test_dbfs_mv_file_is_native(fs, api):
    fs.mv("/t/data.bin", "/t/moved.bin")
    assert api.calls_of("move") == [("move", "/t/data.bin", "/t/moved.bin")]
    assert fs.cat_file("/t/moved.bin") == b"data"


def test_dbfs_mv_folder_whole(fs, api):
    """A recursive move renames a folder whole, as the profile declares: one move."""
    fs.mv("/t/dir", "/t/moved", recursive=True)

    assert api.calls_of("move") == [("move", "/t/dir", "/t/moved")]
    assert fs.find("/t/moved") == ["/t/moved/a.bin", "/t/moved/sub/b.bin"]
    assert not fs.exists("/t/dir")


def test_dbfs_copy(fs):
    fs.copy("/t/dir", "/t/copy", recursive=True)
    assert fs.find("/t/copy") == ["/t/copy/a.bin", "/t/copy/sub/b.bin"]


def test_dbfs_open(fs, api):
    with fs.open("/t/new.bin", "wb") as f:
        f.write(b"new")
    assert api.data["/t/new.bin"] == b"new"

    with pytest.raises(FileExistsError):
        fs.open("/t/new.bin", "xb")
    with pytest.raises(IsADirectoryError):
        fs.open("/t/dir", "wb")
    with pytest.raises(OSError):
        fs.open("/t/new.bin", "ab")

    async def read():
        f = await fs.open_async("/t/new.bin")
        async with f:
            assert (f.mode, f.size) == ("rb", 3)
            return await f.read()

    assert sync(fs.loop, read) == b"new"
    assert fs.cat_file("/t/new.bin") == b"new"


_MiB = 1024 * 1024


def test_dbfs_open_requests(fs, api):
    """A small file is one put; a large one a stream of add-blocks of 1 MiB at most, in order."""
    with fs.open("/t/small.bin", "wb") as f:
        f.write(b"small")
    assert api.calls_of("put") == [("put", "/t/small.bin", 5, True)]
    assert api.calls_of("create") == []

    api.calls.clear()
    data = randbytes(3 * _MiB + 12345)
    with fs.open("/t/large.bin", "wb") as f:
        for i in range(0, len(data), 300_000):
            f.write(data[i : i + 300_000])
    assert api.data["/t/large.bin"] == data
    assert api.calls_of("create") == [("create", "/t/large.bin", True)]
    blocks = [c[2] for c in api.calls_of("add_block")]
    assert sum(blocks) == len(data) and max(blocks) <= _MiB
    assert len(api.calls_of("close")) == 1

    api.calls.clear()
    with fs.open("/t/large.bin", "rb") as f:
        assert f.seekable()
        f.seek(2 * _MiB + 7)
        assert f.read(100) == data[2 * _MiB + 7 : 2 * _MiB + 107]
        assert f.tell() == 2 * _MiB + 107
        f.seek(0)
        assert f.read() == data
    # Reads stay within the API's limit of 1 MB.
    assert max(c[3] for c in api.calls_of("read")) <= _MiB


def test_dbfs_open_exclusive(fs, api):
    with fs.open("/t/x.bin", "xb") as f:
        f.write(randbytes(2 * _MiB))
    assert api.calls_of("create") == [("create", "/t/x.bin", False)]
    with pytest.raises(FileExistsError):
        fs.open("/t/x.bin", "xb")


def test_dbfs_open_aborts(fs, api):
    """A stream cannot be aborted: the file it replaced is deleted, and a small file never sent."""
    with pytest.raises(ValueError, match="dummy"):
        with fs.open("/t/data.bin", "wb") as f:
            f.write(b"lost")
            raise ValueError("dummy")
    assert api.data["/t/data.bin"] == b"data"

    with pytest.raises(ValueError, match="dummy"):
        with fs.open("/t/data.bin", "wb") as f:
            f.write(randbytes(3 * _MiB))
            raise ValueError("dummy")
    assert not fs.exists("/t/data.bin")
    assert api.handles == {}

    api.fail_add_block = True
    with pytest.raises(OSError):
        with fs.open("/t/failed.bin", "wb") as f:
            f.write(randbytes(3 * _MiB))
    assert not fs.exists("/t/failed.bin")
    assert api.handles == {}


def test_dbfs_transaction(fs, api):
    """A file written in a transaction goes to a temporary path, moved into place on commit."""
    with fs.transaction:
        with fs.open("/t/data.bin", "wb") as f:
            f.write(b"replaced")
        assert api.data["/t/data.bin"] == b"data"
    assert api.data["/t/data.bin"] == b"replaced"
    assert [p for p in api.data if "/." in p] == []

    with pytest.raises(ValueError, match="dummy"), fs.transaction:
        with fs.open("/t/lost.bin", "wb") as f:
            f.write(b"lost")
        raise ValueError("dummy")
    assert sorted(api.data) == ["/t/data.bin", "/t/dir/a.bin", "/t/dir/sub/b.bin"]

    # Exclusive creation fails on commit: the API's move refuses the file there.
    with pytest.raises(FileExistsError), fs.transaction:
        with fs.open("/t/data.bin", "xb") as f:
            f.write(b"x")
    assert api.data["/t/data.bin"] == b"replaced"
    assert [p for p in api.data if "/." in p] == []
