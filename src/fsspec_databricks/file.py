import errno
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from io import BytesIO, RawIOBase, UnsupportedOperation
from tempfile import NamedTemporaryFile
from typing import IO, Any, TypeVar

from .error import file_exists_error, file_not_found_error, io_error, os_error

T = TypeVar("T")


class AbstractFile(RawIOBase):
    """Base class of file-like objects."""

    log: logging.Logger = logging.getLogger(__name__)

    verbose_debug_log: bool = False
    """Whether to enable verbose debug logging for file operations."""

    def __init__(
        self,
        path: str,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__()
        self.path = path

        if verbose_debug_log is not None:
            self.verbose_debug_log = verbose_debug_log

    def _ensure_not_closed(self) -> bool:
        """Raise an exception if the file is not ready for reading/writing. Otherwise, return True.

        Returns
        -------
        bool
            Always returns True.
        """
        if self.closed:
            raise ValueError(f"I/O operation on closed file: path={self.path}")

        return True

    def close(self):
        if not self.closed:
            super().close()
            self.log.debug("Closed: path=%s", self.path)

    def __repr__(self):
        return f"{self.__class__.__name__}(path={self.path})"


class AbstractCachedFile(AbstractFile, ABC):
    """Abstract file-like object that supports reading/writing a remote file by caching the entire file content
    in a local resource."""

    max_size: int | None = None
    """The maximum size of the file to read or write. If None, there is no limit on the file size."""

    block_size: int = 1024 * 1024  # 1 MB
    """The size of data block to use when downloading/uploading the remote file content."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(path=path, verbose_debug_log=verbose_debug_log)

        self.max_size = max_size or self.max_size
        self.block_size = block_size or self.block_size

        self.mode = mode
        self._readable = read_mode(mode)
        self._writable = write_mode(mode)

        self._cache = None
        self._updated = False

        if max_size is not None and max_size <= 0:
            raise ValueError("max_size must be a positive integer or None")

        if "x" in mode and self._do_remote_file_exists():
            raise file_exists_error(path)

        self._cache = self._initialize_cache()
        try:
            if "r" in mode or "a" in mode:
                if self._do_remote_file_exists():
                    self._download_remote_file_to_cache()
                    if "r" in mode:
                        self._cache.seek(0)
                elif "r" in mode:
                    raise file_not_found_error(path)
        except BaseException:
            # Eagerly release cache resources
            self._release_cache()
            raise

    @abstractmethod
    def _initialize_cache(self) -> IO[bytes]:
        """Initialize the local cache resource for the file content and return the resource object."""

    @abstractmethod
    def _remote_file_exists(self) -> bool:
        """Check if the remote file exists."""

    @abstractmethod
    def _remote_file_download(self) -> Any:
        """Download the remote file and return a readable file-like object for the downloaded content."""

    @abstractmethod
    def _remote_file_upload(self, data: IO[bytes]) -> None:
        """Upload the given data from a readable file-like object to the remote file."""

    def _do_remote_file_exists(self) -> bool:
        try:
            return self._remote_file_exists()
        except Exception as e:
            raise io_error(
                self.path, message="Failed to check if remote file exists"
            ) from e

    def _do_remote_file_download(self) -> Any:
        try:
            return self._remote_file_download()
        except Exception as e:
            raise io_error(
                self.path, message="Failed to download remote file to cache"
            ) from e

    def _do_remote_file_upload(self, data: IO[bytes]) -> None:
        try:
            self._remote_file_upload(data)
        except Exception as e:
            raise io_error(
                self.path, message="Failed to upload cache to remote file"
            ) from e

    @abstractmethod
    @contextmanager
    def _cached_data(self) -> Generator[IO[bytes], None, None]:
        """Return a file-like object for reading cached data"""

    def _release_cache(self) -> None:
        """Release the cache resource."""
        if self._cache:
            try:
                self._cache.close()
            finally:
                self._cache = None

    def _download_remote_file_to_cache(self) -> None:
        self.log.debug("Downloading remote file to cache: path=%s", self.path)

        with self._do_remote_file_download() as src:
            while data := src.read(self.block_size):
                new_size = self._cache.tell() + len(data)
                if self.max_size and new_size > self.max_size:
                    raise os_error(OSError, errno.EFBIG, self.path)

                self._cache.write(data)

        self.log.debug(
            "Remote file downloaded to cache: path=%s, size=%s",
            self.path,
            self._cache.tell(),
        )

    def _upload_cache_to_remote_file(self) -> None:
        self.log.debug("Uploading cache to remote file: path=%s", self.path)

        with self._cached_data() as data:
            self._do_remote_file_upload(data)

        self.log.debug("Uploaded cache to remote file: path=%s", self.path)

    def close(self):
        if not self.closed:
            try:
                try:
                    self.flush(upload=True)
                finally:
                    super().close()
            finally:
                self._release_cache()

    def readable(self) -> bool:
        """Return True if the file can be read from."""
        return self._readable and self._ensure_not_closed()

    def writable(self) -> bool:
        """Return True if the file can be written to."""
        return self._writable and self._ensure_not_closed()

    def seekable(self) -> bool:
        """Return True if the file supports random access."""
        return self._ensure_not_closed()

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from the file and return them.
        If size is negative or omitted, read all until EOF."""
        self._ensure_not_closed()

        if not self.readable():
            raise UnsupportedOperation(f"File not open for reading: path={self.path}")

        return self._cache.read(size)

    def write(self, data: bytes) -> int:
        """Write the given bytes-like object, and return the number of bytes written."""
        self._ensure_not_closed()

        if not self.writable():
            raise UnsupportedOperation(f"File not open for writing: path={self.path}")

        if self.max_size and self._cache.tell() + len(data) > self.max_size:
            raise os_error(OSError, errno.EFBIG, self.path)

        length = self._cache.write(data)
        if length:
            self._updated = True
        return length

    def flush(self, upload: bool = False):
        """Flush the write buffers of the stream if applicable.

        Parameters
        ----------
        upload : bool, optional
            Whether to upload the cached data to the remote file after flushing the cache, by default False.
            If True, the cached data will be uploaded to the remote file if there are any updates in the cache.
        """
        if not self.writable():
            return

        self._ensure_not_closed()

        if self._cache:
            self._cache.flush()

            if self._updated and upload:
                self._upload_cache_to_remote_file()
                self._updated = False

    def tell(self) -> int:
        """Return the current stream position."""
        self._ensure_not_closed()
        return self._cache.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change the stream position to the given byte offset, interpreted relative to the position
        indicated by whence, and return the new absolute position."""
        self._ensure_not_closed()
        return self._cache.seek(offset, whence)

    def truncate(self, size=None, /):
        """Resize the stream to the given size in bytes."""
        self._ensure_not_closed()
        return self._cache.truncate(size)


class MemoryCachedFile(AbstractCachedFile, ABC):
    """A cached file implementation that uses an in-memory bytes buffer as the cache."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

    def _initialize_cache(self):
        if self.max_size is None:
            raise ValueError(
                "max_size must be specified for MemoryCachedFile to prevent OOM."
            )
        return BytesIO()

    @contextmanager
    def _cached_data(self):
        yield BytesIO(self._cache.getbuffer())


class FileCachedFile(AbstractCachedFile, ABC):
    """A cached file implementation that uses a temporary file on disk as the cache."""

    def __init__(
        self,
        path: str,
        mode="rb",
        max_size: int | None = None,
        block_size: int | None = None,
        verbose_debug_log: bool | None = None,
    ):
        super().__init__(
            path=path,
            mode=mode,
            max_size=max_size,
            block_size=block_size,
            verbose_debug_log=verbose_debug_log,
        )

    def _initialize_cache(self):
        cache = NamedTemporaryFile(delete=False, mode="w+b")  # noqa: SIM115
        self._cache_file_name = cache.name
        return cache

    @contextmanager
    def _cached_data(self):
        pos = self._cache.tell()
        self._cache.close()

        try:
            with open(self._cache_file_name, "rb") as f:
                yield f
        finally:
            self._cache = open(self._cache_file_name, "r+b")  # noqa: SIM115
            self._cache.seek(pos)

    def _release_cache(self) -> None:
        if self._cache:
            try:
                super()._release_cache()
            finally:
                if os.path.exists(self._cache_file_name):
                    os.remove(self._cache_file_name)


def read_mode(mode: str) -> bool:
    """Return True if the given file mode allows reading from the file, False otherwise."""
    return "r" in mode or "+" in mode


def write_mode(mode: str) -> bool:
    """Return True if the given file mode allows writing to the file, False otherwise."""
    return "w" in mode or "a" in mode or "x" in mode or "+" in mode


__all__ = [
    "AbstractCachedFile",
    "FileCachedFile",
    "MemoryCachedFile",
    "read_mode",
    "write_mode",
]
