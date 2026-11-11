# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Existing filesystems, wrapped as a `TreeFileSystem`.

`AsyncTreeWrapper` and `SyncTreeWrapper` wrap an async or a sync filesystem,
so that its recursive methods are the tree strategies. They list a tree a
directory per request, by the wrapped one's ``ls``.

This module depends on `_base`, and `filesystem` of `tree`.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ..tree.filesystem import TreeFileSystem
from ._base import _AsyncFileSystemWrapper, _SyncFileSystemWrapper

if TYPE_CHECKING:
    import asyncio

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.profile import StorageProfile

__all__ = ["AsyncTreeWrapper", "SyncTreeWrapper"]


class AsyncTreeWrapper(_AsyncFileSystemWrapper, TreeFileSystem):
    """An async filesystem, as a `TreeFileSystem`: its recursive methods the tree strategies.

    Its primitives, such as ``_ils``, ``_rm_file`` and ``_mv_file``, are the
    wrapped filesystem's, awaited on its loop. So are its data methods, such
    as ``_cat_file``, and its files. So are the requests on a directory
    whole, each used where the profile says it is one request:

    - ``_rm_tree``, the wrapped one's ``_rm_tree``, e.g. as a
      `TreeFileSystem`, or else its recursive ``_rm``, where
      ``deletes_directories=True``;
    - ``_mv_tree``, the wrapped one's ``_mv_tree``, or else its
      ``_mv_file``, where ``renames_directories=True``.

    The wrapper runs on the same loop, sync or not as the wrapped one is.

    It wins on storages listing one directory per request, as `TreeFileSystem`
    does. It is not cached: each is new, and may be dropped once used.

    Parameters
    ----------
    fs : AsyncFileSystem
        The filesystem to wrap.
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The data methods' calls at a time, such as ``_cat_file``.
        A semaphore is used as it is, shared with what else holds it.
    storage_profile : StorageProfile
        What the storage does. ``UNKNOWN`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`.

    Raises
    ------
    ValueError
        If a concurrency is less than 1.

    See Also
    --------
    SyncTreeWrapper : The same, for a sync filesystem.

    Examples
    --------
    >>> tree = AsyncTreeWrapper(
    ...     fs,
    ...     operation_concurrency=32,
    ...     io_concurrency=8,
    ...     storage_profile=UNKNOWN,
    ... )
    >>> with monitoring(Progress()):
    ...     tree.rm("data/2024", recursive=True)
    """

    def __init__(
        self,
        fs: AsyncFileSystem,
        *,
        operation_concurrency: int | asyncio.Semaphore,
        io_concurrency: int | asyncio.Semaphore,
        storage_profile: StorageProfile,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fs,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            # A wrapper is a tree filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="tree"),
            **kwargs,
        )

    async def _rm_tree(self, path: str) -> None:
        # The wrapped one's own recursive delete, as deletes_directories declares.
        rm_tree = getattr(self.fs, "_rm_tree", None)
        if rm_tree is not None:
            await rm_tree(path)
        else:
            await self.fs._rm(path, recursive=True)

    async def _mv_tree(self, path1: str, path2: str) -> None:
        # The wrapped one's own rename of a directory, as renames_directories declares.
        mv_tree = getattr(self.fs, "_mv_tree", None)
        if mv_tree is not None:
            await mv_tree(path1, path2)
        else:
            await self.fs._mv_file(path1, path2)


class SyncTreeWrapper(_SyncFileSystemWrapper, TreeFileSystem):
    """A sync filesystem, as a `TreeFileSystem`: its recursive methods the tree strategies.

    Its primitives, such as ``_ils``, ``_rm_file`` and ``_mv_file``, call the
    wrapped filesystem's, each in a thread, so that several run at once. Its
    ``_mv_file`` and ``_mv_tree`` are the wrapped one's ``mv``, e.g. a local
    rename, and its ``_rm_tree`` its recursive ``rm``. They move or delete a
    directory whole where the profile's ``renames_directories=True`` or
    ``deletes_directories=True`` says it is one request. Its data methods call
    the wrapped one's too, and its files are the wrapped one's, awaited through
    a `SyncFileAdapter`.

    It wins on storages listing one directory per request, such as a local disk
    or SFTP, as `TreeFileSystem` does. A filesystem unsafe to call from several
    threads needs both concurrencies at 1. It is not cached: each is new, and
    may be dropped once used.

    Parameters
    ----------
    fs : AbstractFileSystem
        The filesystem to wrap.
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time, each in a thread.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The data methods' calls at a time, such as ``_cat_file``.
        A semaphore is used as it is, shared with what else holds it.
    storage_profile : StorageProfile
        What the storage does. ``UNKNOWN`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If a concurrency is less than 1.

    See Also
    --------
    AsyncTreeWrapper : The same, for an async filesystem.

    Examples
    --------
    >>> local = fsspec.filesystem("file")
    >>> tree = SyncTreeWrapper(
    ...     local,
    ...     operation_concurrency=8,
    ...     io_concurrency=4,
    ...     storage_profile=StorageProfile(renames_directories=True),
    ... )
    >>> tree.mv("data/2024", "archive/2024", recursive=True)
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        *,
        operation_concurrency: int | asyncio.Semaphore,
        io_concurrency: int | asyncio.Semaphore,
        storage_profile: StorageProfile,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fs,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            # A wrapper is a tree filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="tree"),
            **kwargs,
        )

    async def _rm_tree(self, path: str) -> None:
        # The wrapped one's own recursive delete, as deletes_directories declares.
        await self._run_in_thread(self.fs.rm, path, recursive=True)

    async def _mv_tree(self, path1: str, path2: str) -> None:
        # The wrapped one's own move, e.g. a local rename, as renames_directories declares.
        await self._run_in_thread(self.fs.mv, path1, path2)
