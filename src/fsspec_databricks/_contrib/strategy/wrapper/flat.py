# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Existing filesystems, wrapped as a `FlatFileSystem`.

`AsyncFlatWrapper` and `SyncFlatWrapper` wrap an async or a sync filesystem,
so that its recursive methods are the flat strategies.

fsspec has no listing by prefix in pages, which the flat strategies list with.
A flat wrapper's recursive ``_ils`` lists with the wrapped filesystem's
``find`` of the directory, which object stores answer in one paginated
listing. That costs twice:

- ``find`` returns the whole listing at once, so the strategies start only
  once all of it has arrived, and it is held in memory, not streamed.
- It lists the whole directory: for "data" with the prefix "2024-", all of
  "data/", the keys not starting with the prefix then left out.

A subclass overriding ``_ils`` with the storage's own listing by prefix, e.g.
from its SDK, streams it page by page, and lists only the prefix.

This module depends on `_base`, and `filesystem` of `flat`.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ..flat.filesystem import FlatFileSystem
from ._base import _AsyncFileSystemWrapper, _SyncFileSystemWrapper

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.profile import StorageProfile

__all__ = ["AsyncFlatWrapper", "SyncFlatWrapper"]


class AsyncFlatWrapper(_AsyncFileSystemWrapper, FlatFileSystem):
    """An async filesystem, as a `FlatFileSystem`: its recursive methods the flat strategies.

    Its primitives, such as ``_info``, ``_cp_file`` and ``_mv_file``, are the
    wrapped filesystem's, awaited on its loop. So are its data methods, such
    as ``_cat_file``, and its files. The wrapper runs on the same loop, sync or
    not as the wrapped one is.

    Its recursive ``_ils`` lists with the wrapped one's ``_find`` of the
    directory. Its ``_rm_files`` gives a batch to the wrapped one's ``_rm``,
    which s3fs, e.g., deletes in one request. That ``_rm`` tells no key
    apart, so any failure but a missing key fails the whole batch.

    It wins on object stores, as `FlatFileSystem` does. It is not cached: each
    is new, and may be dropped once used.

    Parameters
    ----------
    fs : AsyncFileSystem
        The filesystem to wrap.
    rm_batch_size : int
        The most keys the wrapped ``_rm`` deletes in one request, e.g. 1000 on
        S3.
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The data methods' calls at a time, such as ``_cat_file``.
        A semaphore is used as it is, shared with what else holds it.
    storage_profile : StorageProfile
        What the storage does. ``StorageProfile()`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`.

    Raises
    ------
    ValueError
        If a concurrency, or ``rm_batch_size``, is less than 1.

    See Also
    --------
    SyncFlatWrapper : The same, for a sync filesystem.

    Examples
    --------
    >>> flat = AsyncFlatWrapper(
    ...     s3,
    ...     rm_batch_size=1000,
    ...     operation_concurrency=64,
    ...     io_concurrency=8,
    ...     storage_profile=StorageProfile(),
    ... )
    >>> with monitoring(Progress()):
    ...     flat.rm("bucket/data/2024", recursive=True)
    """

    def __init__(
        self,
        fs: AsyncFileSystem,
        *,
        rm_batch_size: int,
        operation_concurrency: int | asyncio.Semaphore,
        io_concurrency: int | asyncio.Semaphore,
        storage_profile: StorageProfile,
        **kwargs: Any,
    ) -> None:
        self.rm_batch_size = rm_batch_size

        super().__init__(
            fs,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            # A wrapper is a flat filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="flat"),
            **kwargs,
        )

    async def _ils(
        self, path: str, recursive: bool = False, prefix: str = "", **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        if recursive:
            # fsspec has no listing in pages: the wrapped one's find, all at once.
            found = await self.fs._find(path, detail=True, **kwargs)
            listing = list(found.values())
        else:
            listing = await self.fs._ls(path, detail=True, **kwargs)
        for entry in _starting(listing, path, prefix):
            yield entry

    async def _rm_files(self, paths: list[str]) -> None:
        try:
            await self.fs._rm(list(paths))
        except FileNotFoundError:
            # A key gone meanwhile: the rest one by one, a missing key no error.
            await asyncio.gather(*(self._rm_if_there(p) for p in paths))

    async def _rm_if_there(self, path: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            await self.fs._rm_file(path)


class SyncFlatWrapper(_SyncFileSystemWrapper, FlatFileSystem):
    """A sync filesystem, as a `FlatFileSystem`: its recursive methods the flat strategies.

    Its primitives, such as ``_info``, ``_cp_file`` and ``_mv_file``, call the
    wrapped filesystem's, each in a thread, so that several run at once. Its
    ``_mv_file`` is the wrapped one's ``mv``. Its data methods call the
    wrapped one's too, and its files are the wrapped one's, awaited through a
    `SyncFileAdapter`.

    Its recursive ``_ils`` and its ``_rm_files`` are as `AsyncFlatWrapper`'s,
    by the wrapped one's ``find`` and ``rm``. A filesystem unsafe to call from
    several threads needs both concurrencies at 1. It is not cached: each is
    new, and may be dropped once used.

    Parameters
    ----------
    fs : AbstractFileSystem
        The filesystem to wrap.
    rm_batch_size : int
        The most keys the wrapped ``rm`` is given at once.
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time, each in a thread.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The data methods' calls at a time, such as ``_cat_file``.
        A semaphore is used as it is, shared with what else holds it.
    storage_profile : StorageProfile
        What the storage does. ``StorageProfile()`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If a concurrency, or ``rm_batch_size``, is less than 1.

    See Also
    --------
    AsyncFlatWrapper : The same, for an async filesystem.

    Examples
    --------
    >>> flat = SyncFlatWrapper(
    ...     fs,
    ...     rm_batch_size=1000,
    ...     operation_concurrency=8,
    ...     io_concurrency=4,
    ...     storage_profile=StorageProfile(),
    ... )
    >>> flat.find("bucket/data")
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        *,
        rm_batch_size: int,
        operation_concurrency: int | asyncio.Semaphore,
        io_concurrency: int | asyncio.Semaphore,
        storage_profile: StorageProfile,
        **kwargs: Any,
    ) -> None:
        self.rm_batch_size = rm_batch_size

        super().__init__(
            fs,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            # A wrapper is a flat filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="flat"),
            **kwargs,
        )

    async def _ils(
        self, path: str, recursive: bool = False, prefix: str = "", **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        if recursive:
            # fsspec has no listing in pages: the wrapped one's find, all at once.
            found = await self._run_in_thread(self.fs.find, path, detail=True, **kwargs)
            listing = list(found.values())
        else:
            listing = await self._run_in_thread(self.fs.ls, path, detail=True, **kwargs)
        for entry in _starting(listing, path, prefix):
            yield entry

    async def _rm_files(self, paths: list[str]) -> None:
        try:
            await self._run_in_thread(self.fs.rm, list(paths))
        except FileNotFoundError:
            # A key gone meanwhile: the rest one by one, a missing key no error.
            await asyncio.gather(*(self._rm_if_there(p) for p in paths))

    async def _rm_if_there(self, path: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            await self._run_in_thread(self.fs.rm_file, path)


def _starting(
    listing: list[dict[str, Any]], path: str, prefix: str
) -> list[dict[str, Any]]:
    """A listing's entries starting with ``path/prefix``, in name order, as ``_ils`` yields them."""
    if not prefix:
        return sorted(listing, key=lambda e: e["name"])
    start = f"{path}/{prefix}" if path else prefix
    return sorted(
        (e for e in listing if e["name"].startswith(start)), key=lambda e: e["name"]
    )
