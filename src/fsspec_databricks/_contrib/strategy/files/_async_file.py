# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The machinery under the file objects: a loop, a concurrency limit, requests.

This module depends on nothing else in the package.
"""

from __future__ import annotations

import asyncio
import io
import logging
import sys
import warnings
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from fsspec.asyn import sync

if TYPE_CHECKING:
    import concurrent.futures
    from collections.abc import Awaitable, Callable, Coroutine
    from typing import Self

_MiB = 1024 * 1024

# Only for what fails where no caller can be told: see DEVELOPMENT.md's "Logging".
logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# The aborts scheduled, until they end: a loop holds its tasks only weakly.
_aborting: set[concurrent.futures.Future[None]] = set()


class _AsyncFile(io.RawIOBase):
    """A file whose requests run on an asyncio event loop, under a concurrency limit.

    Each sync method, such as ``read``, runs its async counterpart, such as
    ``_read``, on the loop and waits for it, as fsspec's filesystems do. So the
    sync methods cannot be called on the loop itself, where they raise
    RuntimeError: code there awaits the async ones instead, through
    `files.fsspec_async.AsyncFileAdapter`.

    The async counterparts are ``_read``, ``_write``, ``_flush`` and
    ``aclose``. A file not supporting one keeps the default here, which raises
    `io.UnsupportedOperation`. ``seek`` and ``tell`` need none: as in fsspec's
    async files, they must send no request on the loop.

    ``flush(force=True)`` finishes the file as closing does, as fsspec's files
    do: what was written is then shown, e.g. the upload completed. The file
    then allows no more writes, and closing sends nothing more. A class says
    how it finishes in ``_finish``, which frees what the file holds: one whose
    buffer goes then allows no reads either.

    A request holds the concurrency limit while it runs. Shared by files, the
    limit bounds their requests together, and so the memory these hold.

    A file the garbage collector finds open is aborted, not closed: a file
    never closed was never finished, so nothing written is kept. It warns with
    a ResourceWarning, and ``_abort`` undoes its requests, e.g. aborting an
    upload. The collector waits for that, as closing would. On the loop's own
    thread, it runs as a task instead, once the thread returns to the loop.
    With the loop not running, or the interpreter shutting down, nothing can
    undo the requests: they stay, as if the process had died. Stopping the
    loop before exit is its owner's concern, e.g. fsspec's.

    Its parameters are the subclasses' first ones: ``path``, ``loop`` and
    ``concurrency_limit``. A subclass decides what a file given no limit gets,
    as only it knows how many requests a file sends at a time.
    """

    mode: str
    """The mode, e.g. "rb", as fsspec's files have it."""

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        concurrency_limit: asyncio.Semaphore,
    ) -> None:
        super().__init__()

        self.path = path
        self.loop = loop
        self._requests = _Requests(concurrency_limit)

        # Whether flush(force=True) finished the file, or tried to: closing does
        # not try again.
        self._forced = False

    # The conventions of Python's file objects, and the garbage collector's abort.

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.path!r}>"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def __del__(self) -> None:
        # Unlike io.IOBase, never closes: see the class docstring.
        if self.closed:
            return

        if getattr(self, "loop", None) is None:
            # The constructor failed before the loop was set: no request was sent.
            io.RawIOBase.close(self)
            return

        if self._forced:
            # A forced flush finished the file: nothing is left to undo.
            io.RawIOBase.close(self)
            return

        warnings.warn(
            f"Unclosed file {self.path!r}, aborted: nothing written is kept",
            ResourceWarning,
            stacklevel=1,  # A finalizer has no caller to point at.
            source=self,
        )

        try:
            # On the file's loop, nothing can wait for the abort.
            try:
                on_its_loop = asyncio.get_running_loop() is self.loop
            except RuntimeError:
                on_its_loop = False  # No loop runs on this thread.

            if on_its_loop:
                self._do_abort_soon()
            elif self.loop.is_running() and not sys.is_finalizing():
                try:
                    self._sync(self._abort())
                except Exception:
                    # Nobody is there to catch it: requests may stay on the storage.
                    logger.warning(
                        "Aborting the unclosed file %r failed", self.path, exc_info=True
                    )
            # Else nothing can undo the requests, e.g. at exit: they stay, as
            # if the process had died.
        finally:
            io.RawIOBase.close(self)

    # Flushing and closing.

    def flush(self, force: bool = False) -> None:
        """Send nothing, or, if ``force``, finish the file as closing does.

        Unforced, it sends nothing: what a file sends before closing, it sends
        as the writes allow. So io's closing, which flushes, can call it on the
        loop too.

        Raises
        ------
        ValueError
            If forced twice, as fsspec's files do.
        """
        if force:
            self._sync(self._flush(force=True))
        else:
            self._check_open()

    async def _flush(self, force: bool = False) -> None:
        """Flush as ``flush`` does."""
        self._check_open()
        if not force:
            return

        if self._forced:
            raise ValueError(
                f"Force flush cannot be called more than once: {self.path}"
            )

        self._forced = True
        await self._finish()

    async def aclose(self) -> None:
        """Close the file: finish it, unless a forced flush did, and end its requests."""
        if self.closed:
            return
        try:
            if not self._forced:
                await self._finish()
        finally:
            try:
                await self._requests.cancel_all()
            finally:
                super().close()

    def close(self) -> None:
        if self.closed:
            return

        if getattr(self, "loop", None) is None:
            # The constructor failed before the loop was set: no request was sent.
            super().close()
            return

        self._sync(self.aclose())

    # What a subclass overrides, besides io.RawIOBase's own methods.
    #
    # io.RawIOBase's read and write raise a bare NotImplementedError: these
    # raise what its other unsupported operations do, naming the file too.

    async def _read(self, size: int = -1) -> bytes:
        """Read as ``read`` does."""
        raise io.UnsupportedOperation(f"read on {self.path}")

    async def _write(self, data: Any) -> int:
        """Write as ``write`` does."""
        raise io.UnsupportedOperation(f"write on {self.path}")

    async def _finish(self) -> None:
        """Send what shows the file as written, e.g. complete its upload.

        Closing, or a forced flush, calls it once, even for a file only read.
        It frees what the file holds, e.g. a buffer, even if it fails.
        """

    async def _abort(self) -> None:
        """Undo the file's requests, keeping nothing: a subclass adds to it.

        The garbage collector calls it for a file left open. It frees what the
        file holds, as `_finish` does.
        """
        await self._requests.cancel_all()

    # What every file shares, built on those.
    #
    # Waiting on the loop, from another thread.

    def _sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        """Wait for ``coro`` to run on the loop, from another thread.

        It is fsspec's ``sync``, which raises NotImplementedError, a
        RuntimeError, on the loop itself. But fsspec's waits forever on a loop
        not running: this raises RuntimeError instead.
        """
        try:
            if not self.loop.is_running() or sys.is_finalizing():
                # During shutdown, the loop's daemon thread is frozen, yet its
                # loop still looks running: waiting would hang, as on a stopped
                # loop.
                raise RuntimeError(
                    f"Waiting on a loop that is not running: {self.path}"
                )
            return sync(self.loop, lambda: coro)
        except RuntimeError:
            # Refused before it ran, it is closed so as not to warn that it was
            # never awaited. One that ran and raised is closed already.
            coro.close()
            raise

    def _do_abort_soon(self) -> concurrent.futures.Future[None] | None:
        """Close the file without waiting, scheduling `_abort` on the loop.

        It works from any thread, the loop's own too, where nothing can wait
        for the abort. The file sends nothing more, and keeps nothing written.
        It returns the abort's future, or None if the loop is closed, where
        nothing can run.
        """
        future = None
        if not self.loop.is_closed():
            coro = self._abort()
            try:
                future = asyncio.run_coroutine_threadsafe(coro, self.loop)
                _aborting.add(future)
                future.add_done_callback(_aborting.discard)
                future.add_done_callback(self._warn_if_abort_failed)
            except RuntimeError:
                coro.close()  # The loop closed meanwhile.

        io.RawIOBase.close(self)
        return future

    def _warn_if_abort_failed(self, future: concurrent.futures.Future[None]) -> None:
        # The garbage collector's abort, scheduled: nobody awaits its future.
        if future.cancelled():
            return

        error = future.exception()
        if isinstance(error, Exception):
            logger.warning(
                "Aborting the unclosed file %r failed", self.path, exc_info=error
            )

    # The checks before an operation.

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError(f"I/O operation on closed file: {self.path}")

    def _check_not_forced(self) -> None:
        """Refuse a write once a forced flush finished the file, as fsspec's files do."""
        if self._forced:
            raise ValueError(f"Force-flushed, can only close: {self.path}")


@dataclass(eq=False)
class _Range:
    """A request for the byte range [start, end) of a file."""

    start: int
    end: int
    task: asyncio.Task[Any]

    def succeeded(self) -> bool:
        return (
            self.task.done()
            and not self.task.cancelled()
            and self.task.exception() is None
        )


class _Requests:
    """A file's requests, in the order sent, each holding the limit while it runs."""

    def __init__(self, concurrency_limit: asyncio.Semaphore) -> None:
        self.concurrency_limit = concurrency_limit
        self.queue: deque[_Range] = deque()
        # The first request that failed, so that writes stop at once.
        self.error: BaseException | None = None

    async def send(
        self, start: int, end: int, func: Callable[..., Awaitable[Any]], *args: Any
    ) -> None:
        """Send a request once a slot is free: the caller waits for it."""
        await self.concurrency_limit.acquire()
        try:
            task = asyncio.ensure_future(func(*args))
        except BaseException:
            self.concurrency_limit.release()
            raise

        task.add_done_callback(self._done)
        self.queue.append(_Range(start, end, task))

    def _done(self, task: asyncio.Task[Any]) -> None:
        self.concurrency_limit.release()
        if not task.cancelled() and self.error is None:
            self.error = task.exception()

    async def cancel(self, ranges: list[_Range]) -> None:
        for r in ranges:
            r.task.cancel()
        await asyncio.gather(*(r.task for r in ranges), return_exceptions=True)

    async def cancel_all(self) -> None:
        ranges = list(self.queue)
        self.queue.clear()
        await self.cancel(ranges)
