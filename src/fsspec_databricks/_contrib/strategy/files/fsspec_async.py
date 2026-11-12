# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""fsspec's ``open_async``, on top of this package's file objects.

``AsyncFileSystem.open_async`` returns a file whose methods are awaited, with
the API of fsspec's ``AbstractAsyncStreamedFile``. An `AsyncFileAdapter`
gives that API to the file objects of `files.adaptive`, so that a filesystem
implements its requests once, for both ``open`` and ``open_async``. The adapter only awaits what the file implements already: the file objects
stay files, with Python's sync API.

A `SyncFileAdapter` gives the same API to any other file object with Python's
sync API, e.g. a storage SDK's own, running its blocking calls in a thread.

This module depends on nothing else in the package: it imports `_async_file`
only for type checking.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import IO, Self

    from ._async_file import _AsyncFile

__all__ = ["AsyncFileAdapter", "SyncFileAdapter"]


class AsyncFileAdapter:
    """A file object of this package, as fsspec's ``open_async`` returns it.

    As in fsspec's ``AbstractAsyncStreamedFile``:

    - ``read``, ``write``, ``flush`` and ``close`` are awaited. Each awaits the
      file's async counterpart of the sync method, e.g. ``_read``.
    - ``seek``, ``tell``, ``readable``, ``writable`` and ``seekable`` are not:
      they run the file's own, which send no request.
    - ``path``, ``mode``, ``size``, ``loc`` and ``closed`` describe the file.

    Unlike fsspec's, leaving an ``async with`` block by an exception uploads
    nothing of a written file, as leaving a ``with`` block does. Close it: the
    garbage collector aborts a file left open, keeping nothing written.

    Parameters
    ----------
    file : AdaptiveReadableFile or AdaptiveWritableFile
        The file. Its requests run on its ``loop``, which the awaited methods
        must run on, as the filesystem's async methods must.

    Examples
    --------
    >>> async def open_async(self, path, mode="rb", **kwargs):
    ...     return AsyncFileAdapter(MyFile(self, path, mode))
    >>> async with await fs.open_async(path) as f:
    ...     header = await f.read(8)
    ...     f.seek(-8, io.SEEK_END)
    ...     footer = await f.read()
    """

    def __init__(self, file: _AsyncFile) -> None:
        self.file = file

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.file!r}>"

    @property
    def path(self) -> str:
        return self.file.path

    @property
    def mode(self) -> str:
        return self.file.mode

    @property
    def size(self) -> int | None:
        """The file's size: None if not known, e.g. while writing it."""
        return getattr(self.file, "size", None)

    @property
    def closed(self) -> bool:
        return self.file.closed

    @property
    def loc(self) -> int:
        """The position, as fsspec's files keep it."""
        return self.file.tell()

    def readable(self) -> bool:
        return self.file.readable()

    def writable(self) -> bool:
        return self.file.writable()

    def seekable(self) -> bool:
        return self.file.seekable()

    def seek(self, loc: int, whence: int = io.SEEK_SET) -> int:
        return self.file.seek(loc, whence)

    def tell(self) -> int:
        return self.file.tell()

    async def read(self, length: int | None = -1) -> bytes:
        return await self.file._read(-1 if length is None else length)

    async def write(self, data: Any) -> int:
        return await self.file._write(data)

    async def flush(self, force: bool = False) -> None:
        await self.file._flush(force)

    async def close(self) -> None:
        await self.file.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.file.__aexit__(*exc_info)


class SyncFileAdapter:
    """A sync file object, as fsspec's ``open_async`` returns it.

    It gives the API of `AsyncFileAdapter` to any file object with Python's
    sync API, such as a storage SDK's own or fsspec's ``AbstractBufferedFile``.
    Each awaited method runs the file's blocking one in a thread, with
    `asyncio.to_thread`, so the event loop goes on meanwhile:

    - ``read``, ``write``, ``flush`` and ``close`` are awaited, as is leaving an
      ``async with`` block, which runs the file's ``__exit__``.
    - ``seek``, ``tell``, ``readable``, ``writable`` and ``seekable`` are not:
      they run the file's own in the calling thread, so they must send no
      request.
    - ``path``, ``mode``, ``size``, ``loc`` and ``closed`` describe the file.

    The awaited methods run one at a time, in the order awaited, as the file
    is not safe to use from several threads at once. ``seek`` and ``tell``
    raise RuntimeError while one runs: the file's own could wait for the
    thread, blocking the event loop. A thread cannot be stopped: a cancelled
    call waits for its thread to end before it raises CancelledError, so that
    the next call finds the file free.

    The threads are those of the loop's default executor, which bounds how
    many run at a time across every adapter.

    Parameters
    ----------
    file : file object
        A file with Python's sync API, opened in binary mode.
    mode : str, optional
        The file's mode, for a file without ``mode``, e.g. an SDK's handle.
    size : int, optional
        The file's size, for a file without ``size``. None leaves it unknown,
        e.g. while writing.

    See Also
    --------
    AsyncFileAdapter : The same API, for this package's own file objects.

    Examples
    --------
    >>> async def open_async(self, path, mode="rb", **kwargs):
    ...     return SyncFileAdapter(self.client.open(path, mode))

    A storage SDK's handle tells neither its mode nor its size, and opening it
    may block. `open` opens it in a thread:

    >>> async def open_async(self, path, mode="rb", **kwargs):
    ...     size = (await self._info(path))["size"] if "r" in mode else None
    ...     opener = functools.partial(self.client.open, path, mode)
    ...     return await SyncFileAdapter.open(opener, mode=mode, size=size)
    >>> async with await fs.open_async(path) as f:
    ...     header = await f.read(8)
    """

    def __init__(
        self, file: IO[bytes], *, mode: str | None = None, size: int | None = None
    ) -> None:
        self.file = file
        self._mode = mode
        self._size = size
        # Serialises the threads' calls, which the file cannot take at once.
        self._lock = asyncio.Lock()

    @classmethod
    async def open(
        cls,
        opener: Callable[[], IO[bytes]],
        *,
        mode: str | None = None,
        size: int | None = None,
    ) -> Self:
        """Open a file by a blocking call, in a thread, and adapt it.

        Parameters
        ----------
        opener : callable
            Called with no argument, in a thread, it returns the file opened.
        mode, size
            As for `SyncFileAdapter`.

        Returns
        -------
        SyncFileAdapter
            The file opened, adapted.
        """
        # Nothing holds the file yet: a cancelled open leaves it to its thread.
        return cls(await asyncio.to_thread(opener), mode=mode, size=size)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.file!r}>"

    @property
    def path(self) -> str | None:
        """The file's path: fsspec's files have ``path``, Python's ``name``."""
        return getattr(self.file, "path", None) or getattr(self.file, "name", None)

    @property
    def mode(self) -> str:
        """The mode given, or else the file's."""
        return self._mode if self._mode is not None else self.file.mode

    @property
    def size(self) -> int | None:
        """The size given, or else the file's: None if neither tells it."""
        if self._size is not None:
            return self._size
        else:
            return getattr(self.file, "size", None)

    @property
    def closed(self) -> bool:
        return self.file.closed

    @property
    def loc(self) -> int:
        """The position, as fsspec's files keep it."""
        return self.tell()

    def _idle(self) -> IO[bytes]:
        """The file, once checked that no thread runs in it.

        Python's buffered files lock themselves for each call: a seek in the
        loop's thread would wait for a read in another, blocking the loop.
        """
        if self._lock.locked():
            raise RuntimeError(f"A call runs in a thread on {self.path}")
        return self.file

    async def _in_thread(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run one of the file's blocking calls in a thread, after the others."""
        async with self._lock:
            call = asyncio.ensure_future(asyncio.to_thread(func, *args))
            try:
                return await asyncio.shield(call)
            except asyncio.CancelledError:
                # The thread runs on, holding the file: release it only after,
                # however often the wait is cancelled. It raises after all.
                while not call.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        await asyncio.wait([call])

                if not call.cancelled():
                    call.exception()  # Retrieved, as the cancellation wins.
                raise

    def readable(self) -> bool:
        return self.file.readable()

    def writable(self) -> bool:
        return self.file.writable()

    def seekable(self) -> bool:
        return self.file.seekable()

    def seek(self, loc: int, whence: int = io.SEEK_SET) -> int:
        return self._idle().seek(loc, whence)

    def tell(self) -> int:
        return self._idle().tell()

    async def read(self, length: int | None = -1) -> bytes:
        return await self._in_thread(self.file.read, -1 if length is None else length)

    async def write(self, data: Any) -> int:
        return await self._in_thread(self.file.write, data)

    async def flush(self, force: bool = False) -> None:
        """Flush the file: ``force`` only if the file takes it, as fsspec's do."""
        if force:
            await self._in_thread(lambda: self.file.flush(force=True))  # type: ignore[call-arg]
        else:
            await self._in_thread(self.file.flush)

    async def close(self) -> None:
        await self._in_thread(self.file.close)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._in_thread(self.file.__exit__, *exc_info)
