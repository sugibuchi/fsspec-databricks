from io import BytesIO, UnsupportedOperation
from random import randbytes

import pytest

from fsspec_databricks.file import FileCachedFile, MemoryCachedFile

from .utils import bytes_sig


class DummyCachedFileMixin:
    _remote_file: bytes | None = None

    def _remote_file_exists(self):
        return self._remote_file is not None

    def _remote_file_download(self):
        return BytesIO(self._remote_file)

    def _remote_file_upload(self, data):
        self._remote_file = data.read()


class DummyMemoryCachedFile(DummyCachedFileMixin, MemoryCachedFile):
    def __init__(
        self,
        remote_file: bytes | None = None,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        self._remote_file = remote_file

        super().__init__(
            path="/path/to/file",
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )


class DummyFileCachedFile(DummyCachedFileMixin, FileCachedFile):
    def __init__(
        self,
        remote_file: bytes | None = None,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        self._remote_file = remote_file

        super().__init__(
            path="/path/to/file",
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_memory_cached_file_init(file_class):
    data = randbytes(10 * 1024 * 1024)

    with (
        pytest.raises(FileNotFoundError),
        file_class(
            remote_file=None,
            mode="rb",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "rb"
        assert file.max_size == 12 * 1024 * 1024
        assert file.block_size == 3456 * 1024

        assert file.readable()
        assert not file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "wb"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=None,
        mode="ab",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "ab"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == len(data)

        file._cache.seek(0)
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="xb",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "xb"

        assert not file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with (
        pytest.raises(FileExistsError),
        file_class(
            remote_file=data,
            mode="xb",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with (
        pytest.raises(FileNotFoundError),
        file_class(
            remote_file=None,
            mode="r+b",
            max_size=12 * 1024 * 1024,
            block_size=3456 * 1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "r+b"
        assert file.max_size == 12 * 1024 * 1024
        assert file.block_size == 3456 * 1024

        assert file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == data

    with file_class(
        remote_file=None,
        mode="w+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.mode == "w+b"

        assert file.readable()
        assert file.writable()
        assert file.seekable()

        assert file.tell() == 0
        assert file._cache.read() == b""

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=12 * 1024 * 1024,
        block_size=3456 * 1024,
    ) as file:
        assert file.tell() == 0
        assert file._cache.read() == b""


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_read(file_class):
    data = randbytes(1024 * 1024)

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file.read()) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="rb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        data2 = bytearray()
        while chunk := file.read(3456 * 1024):
            data2.extend(chunk)
        assert bytes_sig(data2) == bytes_sig(data)

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="wb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="ab",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=None,
            mode="xb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.read()

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file.read()) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert file.read() == b""


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_write(file_class):
    data = randbytes(1024 * 1024)

    with (
        pytest.raises(UnsupportedOperation),
        file_class(
            remote_file=data,
            mode="rb",
            max_size=10 * 1024 * 1024,
            block_size=34 * 1024,
        ) as file,
    ):
        file.write(b"test")

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        for i in range(0, len(data), 123 * 100):
            file.write(data[i : min(i + 123 * 100, len(data))])

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data + data)

    with file_class(
        remote_file=None,
        mode="xb",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="r+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=10 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        file.write(data)

    assert bytes_sig(file._remote_file) == bytes_sig(data)


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_flush(file_class):
    data = randbytes(1024 * 1024)
    chunk = randbytes(256)

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=12 * 1024 * 1024,
        block_size=34 * 1024,
    ) as file:
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.write(chunk)
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.flush()
        assert bytes_sig(file._remote_file) == bytes_sig(data)

        file.flush(upload=True)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.flush()
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.write(chunk)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk)

        file.flush(upload=True)
        assert bytes_sig(file._remote_file) == bytes_sig(chunk + chunk)

    assert bytes_sig(file._remote_file) == bytes_sig(chunk + chunk)


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_max_init_size(file_class):
    data = randbytes(1024 * 1024)

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="rb",
            max_size=1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="wb",
        max_size=1024,
    ) as _:
        pass

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="ab",
            max_size=1024,
        ) as _,
    ):
        pass

    with (
        pytest.raises(OSError),
        file_class(
            remote_file=data,
            mode="r+b",
            max_size=1024,
        ) as _,
    ):
        pass

    with file_class(
        remote_file=data,
        mode="w+b",
        max_size=1024,
    ) as _:
        pass


@pytest.mark.parametrize(
    "file_class",
    [
        DummyMemoryCachedFile,
        DummyFileCachedFile,
    ],
)
def test_cached_file_max_write_size(file_class):
    data = randbytes(1024 * 1024)
    chunk = randbytes(256)

    with file_class(
        remote_file=None,
        mode="wb",
        max_size=256,
    ) as file:
        file.write(chunk)

        with pytest.raises(OSError):
            file.write(chunk)

    with file_class(
        remote_file=data,
        mode="ab",
        max_size=len(data) + 256,
    ) as file:
        file.write(chunk)

        with pytest.raises(OSError):
            file.write(chunk)


def test_memory_cached_file_max_size_check():
    with (
        pytest.raises(
            ValueError,
            match=r"max_size must be specified for MemoryCachedFile to prevent OOM.",
        ),
        DummyMemoryCachedFile(
            remote_file=None,
            mode="wb",
            # No max_size
        ) as _,
    ):
        pass
