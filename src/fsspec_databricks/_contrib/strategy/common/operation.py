# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Operations: what the strategies return, how one is run to its end, and watched.

A strategy returns an `Operation`. It does its work as it is iterated, and
yields what it did, path by path, mixed with the errors of the paths that
failed. The caller handles each, and can stop the operation as an asyncio task
is stopped, through the methods of the interface's `OperationHandler`.

From what its callers use to what that builds on:

- `Operation` is what a strategy returns.
- `_collect` runs an operation to its end, returning all it yielded.
- `_collected` does so for an fsspec call, as the call's monitors watch it.
- `_watched` runs an operation for an fsspec call, yielding its items, telling
  the monitors of the `monitoring` blocks around it, and fsspec's ``callback=``.
- `_Counting` is the monitor fsspec's ``callback=`` is told through.

This module depends on nothing else in the package.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from fsspec_alt_strategies_common.monitor import (
    CallCancelled,
    Monitor,
    OperationHandler,
    State,
    current_monitors,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from typing import Self

    from fsspec_alt_strategies_common.monitor import Call


__all__ = ["Operation"]

T = TypeVar("T")
S = TypeVar("S", bound=State)


class Operation(OperationHandler, Generic[T, S]):
    """A strategy's work, done as it is iterated, and stopped as a task is.

    Iterating it yields what it did, e.g. each path deleted, and the error of
    each path that failed, as an exception object. An error ending the whole
    operation, such as a missing root, is raised instead.

    It does its work only while iterated, a few requests ahead of the caller.
    So a slow loop body slows it, and an operation never iterated does nothing.

    Its methods follow `asyncio.Task`'s, as `OperationHandler` declares them:

    - `cancel` asks it to stop. It starts no new request, but goes on yielding
      the outcomes of those already sent, then ends.
    - `cancelling` tells whether a stop was asked, and `cancelled` whether it
      ended so.
    - `done` tells whether it ended, whatever the reason.

    Leaving the loop early stops it at once, without yielding what is in
    flight. Use it as an async context manager, so that this happens on
    leaving the block: requests that only read are then cancelled, and those
    that change the storage, e.g. deletions, are waited for.

    Attributes
    ----------
    state : State
        Its progress so far. Each strategy's docstring names its state class.

    Examples
    --------
    >>> rm = tree_rm(
    ...     "data/2024",
    ...     ls=fs._ls,
    ...     info=fs._info,
    ...     rm_file=fs._rm_file,
    ...     rm_folder=fs._rmdir,
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with rm as op:
    ...     async for path in op:
    ...         if isinstance(path, Exception):
    ...             op.cancel()  # Stop, but see what was in flight.
    ...             print("failed:", path)
    >>> op.cancelled(), op.state.files_deleted
    (True, 12)
    """

    def __init__(
        self,
        run: Callable[[Operation[T, S]], AsyncGenerator[T | Exception]],
        state: S,
    ) -> None:
        self.state = state
        self._run = run
        self._steps: AsyncGenerator[T | Exception] | None = None
        self._cancel_requests = 0
        self._done = False

    def __repr__(self) -> str:
        status = (
            "cancelled"
            if self.cancelled()
            else "done"
            if self._done
            else "cancelling"
            if self._cancel_requests
            else "pending"
        )
        return f"<{type(self).__name__} {status} {self.state}>"

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T | Exception:
        if self._done:
            raise StopAsyncIteration
        if self._steps is None:
            if self._cancel_requests:
                # As a task cancelled before it runs: nothing is sent.
                self._done = True
                raise StopAsyncIteration
            self._steps = self._run(self)
        try:
            return await anext(self._steps)
        except BaseException:
            # The end, an error ending the operation, or this task cancelled:
            # each finishes the steps, which are done.
            self._done = True
            raise

    def cancel(self) -> bool:
        """Ask the operation to stop.

        It starts no new request. Iterating it goes on with the outcomes of the
        requests already sent, then ends.

        Returns
        -------
        bool
            False if it is already done, as `asyncio.Task.cancel` returns.
        """
        if self._done:
            return False
        self._cancel_requests += 1
        return True

    def cancelling(self) -> int:
        """The number of stops asked, as `asyncio.Task.cancelling` counts them."""
        return self._cancel_requests

    def cancelled(self) -> bool:
        """Whether it ended after a stop was asked, or was closed before its end."""
        return self._done and self._cancel_requests > 0

    def done(self) -> bool:
        """Whether it ended: run to its end, stopped, closed, or failed."""
        return self._done

    async def aclose(self) -> None:
        """Stop at once, without yielding what is in flight.

        Requests that only read are cancelled. Those that change the storage,
        e.g. deletions, are waited for, as their outcome would otherwise be
        unknown.
        """
        if self._done:
            return
        self._cancel_requests += 1
        try:
            if self._steps is not None:
                await self._steps.aclose()
        finally:
            self._done = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()


async def _collect(
    items: Operation[Any, Any] | AsyncGenerator[Any],
) -> tuple[list[Any], list[Exception]]:
    """Run an operation to its end, and return what it yielded but errors, and its errors.

    Both lists are in the order yielded, which is the order the storage
    answered: callers sort, as fsspec's ``_find`` and ``_glob`` do.
    """
    found: list[Any] = []
    errors: list[Exception] = []
    async with aclosing(items):
        async for item in items:
            if isinstance(item, Exception):
                errors.append(item)
            else:
                found.append(item)
    return found, errors


async def _collected(
    operation: Operation[Any, Any], call: Call, callback: Any = None
) -> tuple[list[Any], list[Exception]]:
    """`_collect` of an operation, as its monitors watch it."""
    return await _collect(_watched(operation, call, callback))


async def _watched(
    operation: Operation[Any, Any], call: Call, callback: Any = None
) -> AsyncGenerator[Any]:
    """Run an operation for a call, yielding its items, as its monitors watch it.

    The monitors are those of the `monitoring` blocks around the call, and
    ``callback``, fsspec's, if given. They are told in order, so if one's
    ``ended`` raises, the monitors after it are not told that the call ended.
    """
    monitors = current_monitors()
    if callback is not None:
        monitors = (*monitors, _Counting(callback))
    error: BaseException | None = None
    try:
        async with operation:
            for monitor in monitors:
                await monitor.started(call, operation.state, operation)
            async for item in operation:
                for monitor in monitors:
                    await monitor.yielded(call, item, operation.state, operation)
                yield item
        if operation.cancelled():
            raise CallCancelled(call, operation.state)
    except GeneratorExit:
        # The caller stopped early, e.g. a walk's: no error of the call.
        raise
    except BaseException as e:
        error = e
        raise
    finally:
        for monitor in monitors:
            await monitor.ended(call, error, operation.state, operation)


class _Counting(Monitor):
    """fsspec's ``callback=``, told of each path done, as fsspec's ``put`` tells it.

    The total is not known ahead, as the operation lists while it works, so the
    callback's size stays as its caller set it.
    """

    def __init__(self, callback: Any) -> None:
        self.callback = callback

    async def yielded(
        self, call: Call, item: Any, state: State, op: OperationHandler
    ) -> None:
        if not isinstance(item, Exception):
            self.callback.relative_update(1)
