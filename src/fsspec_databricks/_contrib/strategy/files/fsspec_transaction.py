# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""fsspec's transactions, on top of file objects that know nothing of them.

fsspec opens a file with ``autocommit=False`` inside ``with fs.transaction:``.
When the block ends, it calls the file's ``commit()``, or its ``discard()`` if
the block raised. This module serves two aims:

- It implements the typical strategies keeping a file invisible until then,
  so that each filesystem does not write its own.
- It adds them on top of file objects that exist already, which need no
  support of their own. Those may be a storage SDK's own file-like objects,
  needing only the methods used, such as ``write`` and ``close``.

A `FileTransaction` holds the strategy, for one file:

- `TempPathTransaction` writes to a temporary path beside the file, renamed
  on commit. It suits any storage, best one renaming in one request. Other
  clients may see the temporary file.
- `LocalBufferTransaction` writes to a local buffer, written to the file on
  commit. It suits any storage, holding the file's bytes locally until then.
- `AdaptiveWritableTransaction` has an
  `~fsspec_alt_strategies_impl.files.adaptive.AdaptiveWritableFile` upload its
  parts, but complete the upload only on commit. It costs nothing more.

A storage with a way of its own gets a subclass of `FileTransaction`.
`TransactionFile` is the file fsspec gets, for any of them. A filesystem's
``_open`` returns it when ``autocommit`` is False:

>>> def _open(self, path, mode="rb", autocommit=True, **kwargs):
...     if autocommit:
...         return MyFile(self, path, mode)
...     opener = lambda p: MyFile(self, p, mode)
...     return TransactionFile(LocalBufferTransaction(path, opener))

A `TransactionFile` is a file: text and compression wrappers work on it. Its
methods are sync, as fsspec's transactions are: call them from a thread other
than the filesystem's loop, as fsspec's sync API. fsspec commits a
transaction's files one after another: a transaction is not atomic across
files.

This module depends on nothing but the standard library: it imports the file
classes only for type checking.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import shutil
import sys
import tempfile
import threading
import uuid
import warnings
from abc import ABC, abstractmethod
from typing import IO, TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .adaptive import AdaptiveWritableFile

__all__ = [
    "AdaptiveWritableTransaction",
    "FileTransaction",
    "LocalBufferTransaction",
    "TempPathTransaction",
    "TransactionFile",
]

_MiB = 1024 * 1024

# Only for what fails where no caller can be told: see DEVELOPMENT.md's "Logging".
logger = logging.getLogger(__name__)


class FileTransaction(ABC):
    """How one file's writes stay invisible until commit.

    A `TransactionFile` calls its steps, each once and in order:
    `start_staging` when opened, `end_staging` when closed, then `commit`. Or
    `discard`, at any time instead. Subclass it for a storage with a way of its
    own.

    Parameters
    ----------
    path : str
        The file's path.

    Examples
    --------
    A storage whose uploads stay invisible until a commit request:

    >>> class NativeTransaction(FileTransaction):
    ...     def __init__(self, fs, path):
    ...         super().__init__(path)
    ...         self.fs = fs
    ...
    ...     def start_staging(self):
    ...         self.upload = self.fs.start_upload(self.path)
    ...         return self.upload.file
    ...
    ...     def end_staging(self):
    ...         self.upload.file.close()
    ...
    ...     def commit(self):
    ...         self.upload.commit()
    ...
    ...     def discard(self):
    ...         self.upload.abort()
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.path!r}>"

    @abstractmethod
    def start_staging(self) -> IO[bytes]:
        """Open the file the reads and writes go to."""

    @abstractmethod
    def end_staging(self) -> None:
        """Finish the writing, making nothing visible, e.g. close the staged file."""

    @abstractmethod
    def commit(self) -> None:
        """Make the finished writing visible."""

    @abstractmethod
    def discard(self) -> None:
        """Lose the writing, finished or not.

        The garbage collector calls it too, for a file left undecided: on any
        thread, and at any point of the code running there.
        """


class TransactionFile(io.RawIOBase):
    """A file whose writes become visible on `commit`, or are lost on `discard`.

    Reads and writes go to the file its transaction staged. Closing finishes
    the writing, as the transaction does, but makes nothing visible: `commit`
    does, closing first if needed. ``flush(force=True)`` finishes the writing
    too, leaving the file open for nothing but closing, as fsspec's files do
    in a transaction. Leaving a ``with`` block by an exception discards.

    A commit that fails is not tried again: the file can then only be
    discarded. The garbage collector discards a file neither committed nor
    discarded, its commit failed or not, warning with a ResourceWarning. On a
    thread running an event loop, which must not wait, the discard runs on a
    thread of its own. A discard failing leaves what was staged, as if the
    process had died. So does the interpreter shutting down, when no discard is
    tried.

    Parameters
    ----------
    transaction : FileTransaction
        The strategy keeping the writes invisible. Its ``start_staging`` is
        called at once.

    Examples
    --------
    >>> f = TransactionFile(LocalBufferTransaction(path, open_file))
    """

    def __init__(self, transaction: FileTransaction) -> None:
        super().__init__()

        self.transaction = transaction
        self.path = transaction.path
        self._file: Any = transaction.start_staging()

        # None while undecided, then "committed" or "discarded". A commit that
        # raised leaves "commit failed": undecided still, but only discardable.
        self._outcome: Literal["committed", "discarded", "commit failed"] | None = None
        # "open" while reads and writes go to the staged file. Then "ended" by
        # closing or a forced flush, or "failed" if that raised: never committed.
        self._staging: Literal["open", "ended", "failed"] = "open"

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.path!r}>"

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError(f"I/O operation on closed file: {self.path}")

    def _method(self, name: str) -> Any:
        """The staged file's method, which a file-like object may lack."""
        self._check_open()
        if self._staging != "open":
            # Force-flushed: the staged file is finished, and may be closed.
            raise ValueError(f"Force-flushed, can only close: {self.path}")

        method = getattr(self._file, name, None)
        if method is None:
            raise io.UnsupportedOperation(f"{name} on {self.path}")
        return method

    def _can(self, query: str, method: str) -> bool:
        # A file-like object lacking e.g. readable() can read if it has read().
        self._check_open()
        if self._staging != "open":
            return False  # Force-flushed: it can only close.
        if (answer := getattr(self._file, query, None)) is not None:
            return answer()
        return hasattr(self._file, method)

    def readable(self) -> bool:
        return self._can("readable", "read")

    def writable(self) -> bool:
        return self._can("writable", "write")

    def seekable(self) -> bool:
        return self._can("seekable", "seek")

    def read(self, size: int | None = -1) -> bytes:
        return self._method("read")(-1 if size is None else size)

    def readall(self) -> bytes:
        return self.read(-1)

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(memoryview(buffer)))
        memoryview(buffer).cast("B")[: len(data)] = data
        return len(data)

    def write(self, data: Any) -> int:
        written = self._method("write")(data)
        # Some SDKs' files return nothing, having written all.
        return len(memoryview(data).cast("B")) if written is None else written

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._method("seek")(offset, whence)

    def tell(self) -> int:
        return self._method("tell")()

    def truncate(self, size: int | None = None) -> int:
        return self._method("truncate")(size)

    def flush(self, force: bool = False) -> None:
        """Flush the staged file, or, if ``force``, finish the writing.

        Forced, it finishes the writing as closing does, as fsspec's files do
        in a transaction. The writing stays invisible until `commit`. The file
        then allows only `close`, `commit` and `discard`.

        Raises
        ------
        ValueError
            If forced twice, as fsspec's files do.
        """
        if force:
            self._check_open()
            if self._staging != "open":
                raise ValueError(
                    f"Force flush cannot be called more than once: {self.path}"
                )
            # Open, the file is undecided: commit and discard close it.
            self._end_staging()
            return

        # io.IOBase.close flushes after end_staging or discard closed the file,
        # or after a discard that failed: a decided file sends nothing.
        if (
            self.closed
            or self._staging != "open"
            or self._outcome is not None
            or getattr(self._file, "closed", False)
        ):
            return

        if (flush := getattr(self._file, "flush", None)) is not None:
            flush()

    def close(self) -> None:
        """Finish the writing. It stays invisible until `commit`."""
        if self.closed:
            return
        try:
            if self._outcome is None and self._staging == "open":
                self._end_staging()
        finally:
            super().close()

    def _end_staging(self) -> None:
        """End the staging, once: one that failed is not tried again."""
        self._staging = "failed"
        self.transaction.end_staging()
        self._staging = "ended"

    def commit(self) -> None:
        """Make the writing visible, closing the file first if needed.

        A commit that raised is not tried again: the file is then only
        discarded, by `discard` or by the garbage collector.

        Raises
        ------
        ValueError
            If the file was discarded, or its writing or an earlier commit
            failed.
        """
        if self._outcome == "committed":
            return
        if self._outcome == "discarded":
            raise ValueError(f"Committing a discarded file: {self.path}")
        if self._outcome == "commit failed":
            # A transaction may have spent what it commits from, e.g. a buffer
            # or an aborted upload: trying again could show a wrong file.
            raise ValueError(f"Committing a file whose commit failed: {self.path}")

        self.close()
        if self._staging != "ended":
            raise ValueError(f"Committing a file whose writing failed: {self.path}")

        # Set before, so that a commit failing midway is never retried.
        self._outcome = "commit failed"
        self.transaction.commit()
        self._outcome = "committed"

    def discard(self) -> None:
        """Lose the writing, whether the file is closed or not, or its commit failed."""
        if self._outcome == "discarded":
            return
        if self._outcome == "committed":
            raise ValueError(f"Discarding a committed file: {self.path}")

        self._outcome = "discarded"
        try:
            self.transaction.discard()
        finally:
            super().close()

    def __exit__(self, exc_type: object, *exc_info: object) -> None:
        if exc_type is not None:
            self.discard()
        else:
            self.close()

    def __del__(self) -> None:
        # Unlike io.IOBase, never just closes: that would finish the writing of
        # a file nobody commits.
        # A file whose commit failed is undecided too: what it staged stays.
        if getattr(self, "_outcome", "unstaged") not in (None, "commit failed"):
            return

        warnings.warn(
            f"Undecided file {self.path!r}, discarded",
            ResourceWarning,
            stacklevel=1,  # A finalizer has no caller to point at.
            source=self,
        )

        if sys.is_finalizing():
            # A discard may wait on a loop whose thread is frozen now: what was
            # staged stays, as if the process had died.
            return

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._discard_quietly()
        else:
            # A loop's thread must never wait, and its loop may be the one the
            # discard waits on. Not a daemon: exit waits for it, while the
            # loops still run.
            threading.Thread(
                target=self._discard_quietly, name=f"discard {self.path}"
            ).start()

    def _discard_quietly(self) -> None:
        # From the garbage collector: nobody is there to catch an error.
        try:
            self.discard()
        except Exception:
            logger.warning(
                "Discarding the undecided file %r failed", self.path, exc_info=True
            )


def _temp_path(path: str) -> str:
    """A hidden path beside ``path``, unique to one transaction's file."""
    parent, _, name = path.rpartition("/")
    temp = f".{name}.{uuid.uuid4().hex[:16]}.tmp"
    return f"{parent}/{temp}" if parent else temp


class TempPathTransaction(FileTransaction):
    """A file written to a temporary path, renamed to its own on commit.

    The temporary path is in the file's directory, hidden by a leading ".", so
    that a rename stays within one directory, container or bucket. Starting the
    staging opens a file there, and ending it closes it, which writes it in
    full. Commit renames it, and discard deletes it.

    It suits any writable file, and any storage: best one renaming in one
    request, where commit is atomic. A rename copying then deleting works too,
    at the cost of copying the file. Other clients may see the temporary file,
    and it stays if the process dies before commit or discard.

    The file writes from its start: the temporary path is new. So appending, or
    exclusive creation, do not apply.

    Parameters
    ----------
    path : str
        The file's path.
    open_file : callable
        Opens a file-like object for writing at the path given: the temporary one.
    rename : callable
        Moves a file from its first path to its second, overwriting it, e.g.
        the storage's own rename.
    delete : callable
        Deletes the file at the path given, raising FileNotFoundError if there
        is none.
    temp_path : str, optional
        The temporary path. A hidden one beside ``path`` if None.

    Examples
    --------
    >>> t = TempPathTransaction(
    ...     path, lambda p: MyFile(fs, p, "wb"), fs.mv, fs.rm_file
    ... )
    >>> f = TransactionFile(t)
    """

    def __init__(
        self,
        path: str,
        open_file: Callable[[str], IO[bytes]],
        rename: Callable[[str, str], object],
        delete: Callable[[str], object],
        *,
        temp_path: str | None = None,
    ) -> None:
        super().__init__(path)

        self.open_file = open_file
        self.rename = rename
        self.delete = delete
        self.temp_path = temp_path or _temp_path(path)
        self._file: IO[bytes] | None = None

    def start_staging(self) -> IO[bytes]:
        self._file = self.open_file(self.temp_path)
        return self._file

    def end_staging(self) -> None:
        if self._file is not None:
            self._file.close()

    def commit(self) -> None:
        self.rename(self.temp_path, self.path)

    def discard(self) -> None:
        try:
            self.end_staging()
        finally:
            # Never written, e.g. if discarded before the file uploaded.
            with contextlib.suppress(FileNotFoundError):
                self.delete(self.temp_path)


class LocalBufferTransaction(FileTransaction):
    """A file written to a local buffer, written to its own on commit.

    The buffer is a `tempfile.SpooledTemporaryFile`: in memory up to
    ``spool_size`` bytes, then on disk. Ending the staging keeps it as it is.
    Nothing is sent before commit, which opens the file and writes the buffer
    into it, from start to end. Discard drops the buffer.

    It suits any writable file, and any storage, and other clients see nothing
    until commit. But the file's bytes are held locally until then, and commit
    writes them all. Reads read the buffer, not the file as it was.

    Parameters
    ----------
    path : str
        The file's path.
    open_file : callable
        Opens a file-like object for writing at the path given, in the mode wanted:
        e.g. "ab" appends the buffer on commit.
    spool_size : int
        The most bytes buffered in memory, beyond which the buffer moves to
        disk.

    Examples
    --------
    >>> t = LocalBufferTransaction(path, lambda p: MyFile(fs, p, "wb"))
    >>> f = TransactionFile(t)
    """

    def __init__(
        self,
        path: str,
        open_file: Callable[[str], IO[bytes]],
        *,
        spool_size: int = 8 * _MiB,
    ) -> None:
        super().__init__(path)
        self.open_file = open_file
        self.spool_size = spool_size
        self._buffer: Any = None

    def start_staging(self) -> IO[bytes]:
        self._buffer = tempfile.SpooledTemporaryFile(max_size=self.spool_size)  # noqa: SIM115
        return self._buffer

    def end_staging(self) -> None:
        pass  # The buffer is kept for commit.

    def commit(self) -> None:
        try:
            self._buffer.seek(0)
            # Only close(): a file-like object may not be a context manager.
            with contextlib.closing(self.open_file(self.path)) as f:
                shutil.copyfileobj(self._buffer, f, _MiB)
        finally:
            self._buffer.close()

    def discard(self) -> None:
        if self._buffer is not None:
            self._buffer.close()


class _Discarded(Exception):
    """Raised in a file's closing, so that the file aborts its upload."""


class AdaptiveWritableTransaction(FileTransaction):
    """An adaptive file uploading its parts on closing, completed on commit.

    The file's last request, the one making it visible, waits for commit: the
    multipart upload's completion, or a small file's upload. The transaction
    stands in for the file's ``_complete_multipart_upload`` and
    ``_upload_all``, on that file only, so as to hold that request back.
    Discard aborts the multipart upload, or drops the small file.

    It needs a storage where an upload is invisible until the request
    completing it, such as S3, Google Cloud Storage or Azure. On one where a
    file is visible while it is written, e.g. DBFS, it hides nothing: use a
    `TempPathTransaction` there.

    It costs nothing more than writing the file without a transaction, and
    other clients see nothing until commit. A small file is held in memory
    until then. Uploaded parts take storage until commit: if the process dies
    first, they stay until the storage expires them.

    On some storages, the last part makes the file visible, e.g. Google Cloud
    Storage's resumable uploads. ``hold_last_part`` holds it back too, in
    memory, until commit uploads it.

    Parameters
    ----------
    file : AdaptiveWritableFile
        The file, open for writing.
    hold_last_part : bool
        Whether the last part waits for commit, for storages where it makes
        the file visible.

    Examples
    --------
    >>> f = TransactionFile(AdaptiveWritableTransaction(MyFile(fs, path)))
    """

    def __init__(
        self, file: AdaptiveWritableFile, *, hold_last_part: bool = False
    ) -> None:
        super().__init__(file.path)

        self.file = file
        self.hold_last_part = hold_last_part

        # The request making the file visible, once closing held it back.
        self._last: Callable[[], Awaitable[None]] | None = None
        self._multipart = False
        self._discarding = False

    def start_staging(self) -> IO[bytes]:
        file = self.file
        complete, upload_all = file._complete_multipart_upload, file._upload_all
        upload_part = file._upload_part

        # The last part's upload, once closing held it back.
        held: list[Callable[[], Awaitable[Any]]] = []

        async def hold_last_part(
            data: bytes, start: int, end: int, index: int, last: bool
        ) -> Any:
            if not last:
                return await upload_part(data, start, end, index, last)
            held.append(lambda: upload_part(data, start, end, index, last))
            return None  # Replaced on commit by what its upload returns.

        async def hold_completion(parts: list[Any]) -> None:
            if self._discarding:
                raise _Discarded  # The file aborts the upload.

            self._multipart = True

            async def last() -> None:
                if held:
                    parts[-1] = await held[0]()
                await complete(parts)

            self._last = last

        async def hold_upload(data: bytes) -> None:
            self._last = lambda: upload_all(data)

        file._complete_multipart_upload = hold_completion  # type: ignore[method-assign]
        file._upload_all = hold_upload  # type: ignore[method-assign]
        if self.hold_last_part:
            file._upload_part = hold_last_part  # type: ignore[method-assign]

        return file

    def end_staging(self) -> None:
        self.file.close()

    def commit(self) -> None:
        try:
            if self._last is not None:
                self.file._sync(self._last())
        except BaseException as error:
            try:
                self._abort_upload()
            except Exception as abort_error:
                # The commit's error is what the caller acts on: the abort's
                # failure only adds that parts may stay on the storage.
                error.add_note(
                    f"Aborting the upload of {self.path!r} failed: {abort_error!r}"
                )
            raise

    def discard(self) -> None:
        self._discarding = True
        self._last = None

        if not self.file.closed:
            with contextlib.suppress(_Discarded):
                self.file.close()
        else:
            self._abort_upload()

    def _abort_upload(self) -> None:
        if self._multipart:
            self._multipart = False
            self.file._sync(self.file._abort_multipart_upload())
