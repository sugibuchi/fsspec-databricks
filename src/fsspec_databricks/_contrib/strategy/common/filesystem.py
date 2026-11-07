# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""What the filesystem classes of the families share, e.g. `TreeFileSystem`'s.

`_StrategyFileSystem` holds the two concurrency limits, and moves data through
file objects: its ``_cat_file``, ``_pipe_file``, ``_get_file`` and
``_put_file`` open the file with ``open_async``, which a subclass implements.
The functions below it shape the answers and errors of fsspec's methods, as
the families' classes give them.

This module depends on nothing else in the package.
"""

from __future__ import annotations

import asyncio
import functools
import os
from abc import ABCMeta
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK

from fsspec_alt_strategies_common.monitor import Monitored
from fsspec_alt_strategies_common.profile import StorageProfile

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from fsspec.callbacks import Callback

_MiB = 1024 * 1024


class _AbstractFileSystemMeta(type(AsyncFileSystem), ABCMeta):  # type: ignore[misc]
    """fsspec's metaclass, which caches instances, with ABCMeta's abstract methods.

    A filesystem class has fsspec's metaclass already. ABCMeta must join it, so
    that a subclass missing an abstract method fails at once, when made.
    """


class _StrategyFileSystem(
    AsyncFileSystem, Monitored, metaclass=_AbstractFileSystemMeta
):
    """An async filesystem with two concurrency limits, moving data through files.

    ``operation_limit`` bounds the requests of the strategies, such as
    listings, deletions and copies. ``io_limit`` bounds the reads and writes of
    data: ``open_async`` gives it to the files it opens, as their
    ``concurrency_limit``. Each is an `asyncio.Semaphore`, its own per
    instance.

    ``storage_profile``, a class field, tells the strategies what the storage
    does. A subclass must set it: one whose storage is unknown says so, with
    ``StorageProfile()``.

    Each family's class declares the abstract methods its subclasses implement,
    so that a reader finds them all in one place.
    """

    transfer_size: int = 8 * _MiB
    """The bytes read or written at a time by ``_get_file`` and ``_put_file``."""

    def __init__(
        self,
        *args: Any,
        operation_concurrency: int,
        io_concurrency: int,
        **kwargs: Any,
    ) -> None:
        for name, value in (
            ("operation_concurrency", operation_concurrency),
            ("io_concurrency", io_concurrency),
        ):
            if value < 1:
                raise ValueError(f"{name} must be at least 1: {value}")
        super().__init__(*args, **kwargs)
        self.operation_concurrency = operation_concurrency
        self.io_concurrency = io_concurrency
        profile = getattr(self, "storage_profile", None)
        if not isinstance(profile, StorageProfile):
            raise ValueError(
                f"storage_profile must be set, to a StorageProfile: {profile!r}"
            )
        self.operation_limit = asyncio.Semaphore(operation_concurrency)
        self.io_limit = asyncio.Semaphore(io_concurrency)

    def _unwrapped(self) -> AsyncFileSystem:
        """The filesystem a monitor is told of: this one, or the one a wrapper wraps."""
        return self

    def _plain(self, path: str | Sequence[str]) -> Any:
        """A path, or paths, without the protocol, keeping a trailing "/".

        The strategies take paths without a protocol. A trailing "/" means
        something to some, e.g. a glob of folders only, or a copy into a folder.
        """
        if not isinstance(path, str):
            return [self._plain(p) for p in path]
        plain = self._strip_protocol(path)
        return plain + "/" if path.endswith("/") and not plain.endswith("/") else plain

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        f = await self.open_async(path, "rb", **kwargs)
        async with f:
            if start is not None:
                f.seek(start if start >= 0 else max(0, f.size + start))
            if end is not None:
                if end < 0:
                    end = f.size + end
                return await f.read(end - f.tell())
            return await f.read()

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any
    ) -> None:
        f = await self.open_async(path, "xb" if mode == "create" else "wb", **kwargs)
        async with f:
            await f.write(value)

    async def _get_file(
        self,
        rpath: str,
        lpath: str,
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        if os.path.isdir(lpath):  # noqa: ASYNC240, as the local open below.
            return  # A folder, which fsspec's get makes itself.
        f = await self.open_async(rpath, "rb", **kwargs)
        async with f:
            # The bytes, as fsspec's own filesystems tell them to the callback.
            callback.set_size(f.size)
            # Local writes are short next to a storage's requests, as in fsspec's
            # own filesystems.
            with open(lpath, "wb") as local:  # noqa: ASYNC230
                while chunk := await f.read(self.transfer_size):
                    local.write(chunk)
                    callback.relative_update(len(chunk))

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        mode: str = "overwrite",
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        if os.path.isdir(lpath):  # noqa: ASYNC240, as the local open below.
            return  # A folder: a storage needs none for the files put in it.
        f = await self.open_async(rpath, "xb" if mode == "create" else "wb", **kwargs)
        async with f:
            with open(lpath, "rb") as local:  # noqa: ASYNC230
                callback.set_size(os.fstat(local.fileno()).st_size)
                while chunk := local.read(self.transfer_size):
                    await f.write(chunk)
                    callback.relative_update(len(chunk))


def _with(func: Callable[..., Any], kwargs: dict[str, Any]) -> Callable[..., Any]:
    """``func``, given ``kwargs`` too at each call, e.g. ``_ls`` with refresh=True.

    fsspec's recursive methods pass their extra keyword arguments on to the
    listing, which the strategies call with the path and ``detail`` alone.
    """
    return functools.partial(func, **kwargs) if kwargs else func


def _answer(entries: list[dict[str, Any]], detail: bool) -> Any:
    """What fsspec's ``_find`` and ``_glob`` answer: the paths sorted, or their entries."""
    found = {e["name"].rstrip("/"): e for e in entries}
    names = sorted(found)
    return {n: found[n] for n in names} if detail else names


def _du_answer(size: int, entries: list[dict[str, Any]], total: bool) -> Any:
    """What fsspec's ``_du`` answers: the total size, or each path's."""
    if total:
        return size
    return {e["name"]: e.get("size") or 0 for e in entries}


def _copy_errors(errors: list[Exception], on_error: str | None) -> list[Exception]:
    """The errors of a recursive copy, as fsspec's raises them.

    A source gone meanwhile is no error, unless ``on_error`` is "raise".
    """
    if on_error == "raise":
        return errors
    return [e for e in errors if not isinstance(e, FileNotFoundError)]


def _raise(errors: list[Exception], doing: str) -> None:
    """Raise the errors an operation yielded: one alone, several in a group."""
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"{doing} failed for {len(errors)} paths", errors)


def _failed_folder(
    folder: str,
    error: Exception,
    on_error: str | Callable[[Exception], Any],
    detail: bool,
) -> tuple[str, Any, Any]:
    """What fsspec's ``walk`` yields for a folder that failed to list, the root too."""
    if on_error == "raise":
        raise error
    if callable(on_error):
        on_error(error)
    return (folder, {}, {}) if detail else (folder, [], [])
