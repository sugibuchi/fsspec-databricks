# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Blocking calls of a sync client, run in a filesystem's own threads.

An async filesystem over a sync client, e.g. an SDK's or a sync fsspec
filesystem, runs each blocking call in a thread, so that its event loop goes
on. `SyncClientSupport` gives it threads of its own, as many as its requests
at a time: the loop's default executor, shared with all else, is then never
taken up by one filesystem, nor shared with another.

This module depends on nothing else in the package.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["SyncClientSupport"]

_T = TypeVar("_T")


class SyncClientSupport:
    """A mixin running a sync client's blocking calls in a filesystem's own threads.

    ``_run_in_thread`` runs a call in one of the threads, made on first use.
    There are as many as ``_thread_count`` says: by default the filesystem's
    ``operation_concurrency``, if a number, as `TreeFileSystem` and
    `FlatFileSystem` take it. A semaphore shared with others leaves the count
    to the executor's default.

    ``close()`` shuts the threads down, without waiting, cancelling the calls
    not started. So does collecting the filesystem. A call after makes them
    again.

    Examples
    --------
    >>> class MyFileSystem(SyncClientSupport, TreeFileSystem):
    ...     async def _info(self, path, **kwargs):
    ...         return await self._run_in_thread(self.client.stat, path)
    """

    _executor: ThreadPoolExecutor | None = None

    def _thread_count(self) -> int | None:
        """How many threads to make: None leaves it to the executor's default."""
        concurrency = getattr(self, "operation_concurrency", None)
        return concurrency if isinstance(concurrency, int) else None

    async def _run_in_thread(
        self, func: Callable[..., _T], /, *args: Any, **kwargs: Any
    ) -> _T:
        """Run a blocking call in one of the filesystem's own threads, and await it.

        The call's context goes with it, e.g. the monitors of the operation it
        is part of.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._thread_pool(),
            contextvars.copy_context().run,
            functools.partial(func, *args, **kwargs),
        )

    def _thread_pool(self) -> ThreadPoolExecutor:
        """The filesystem's own threads, made on first use."""
        executor = self._executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=self._thread_count(),
                thread_name_prefix=type(self).__name__,
            )
            # Collecting the filesystem shuts the threads down, without the
            # finalizer keeping it alive.
            weakref.finalize(self, executor.shutdown, wait=False, cancel_futures=True)
            self._executor = executor
        return executor

    def _shutdown_threads(self) -> None:
        """Shut the threads down, without waiting: the calls not started are cancelled."""
        executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def close(self) -> None:
        """Shut the threads down, then close as the next class does, if it has a ``close()``.

        `TreeFileSystem` and `FlatFileSystem` have one. fsspec's own classes
        have none.
        """
        self._shutdown_threads()
        close = getattr(super(), "close", None)
        if close is not None:
            close()
