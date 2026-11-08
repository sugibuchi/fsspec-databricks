# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Existing filesystems, wrapped as a `TreeFileSystem`, to use the tree strategies.

`AsyncTreeWrapper` wraps an async filesystem, `SyncTreeWrapper` a sync one. A
wrapper is light, and not cached: make one on demand, e.g. to watch a call
with a monitor, and drop it after. The developer chooses the wrapper: none is
chosen for a filesystem.

This module depends on `filesystem`, and `wrapper` of `common`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..common.wrapper import _AsyncFileSystemWrapper, _SyncFileSystemWrapper
from .filesystem import TreeFileSystem

if TYPE_CHECKING:
    import asyncio

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies_common.profile import StorageProfile

__all__ = ["AsyncTreeWrapper", "SyncTreeWrapper"]


class AsyncTreeWrapper(_AsyncFileSystemWrapper, TreeFileSystem):
    """An async filesystem, as a `TreeFileSystem`: its recursive methods the tree strategies.

    Its primitives, such as ``_ls``, ``_rm_file`` and ``_mv_file``, are the
    wrapped filesystem's, awaited on its loop. So are its data methods, such
    as ``_cat_file``, and its files. So is ``_rm_tree``, a recursive delete,
    when the wrapped one has it, e.g. as a `TreeFileSystem`. The wrapper runs
    on the same loop, sync or not as the wrapped one is.

    It wins on storages listing one folder per request, as `TreeFileSystem`
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
        What the storage does. ``StorageProfile()`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`.

    See Also
    --------
    SyncTreeWrapper : The same, for a sync filesystem.

    Examples
    --------
    >>> tree = AsyncTreeWrapper(
    ...     fs,
    ...     operation_concurrency=32,
    ...     io_concurrency=8,
    ...     storage_profile=StorageProfile(),
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
            storage_profile=storage_profile,
            **kwargs,
        )

        # Optional, as on a TreeFileSystem: left None, rm walks every folder.
        rm_tree = getattr(fs, "_rm_tree", None)
        if rm_tree is not None:
            self._rm_tree = rm_tree


class SyncTreeWrapper(_SyncFileSystemWrapper, TreeFileSystem):
    """A sync filesystem, as a `TreeFileSystem`: its recursive methods the tree strategies.

    Its primitives, such as ``_ls``, ``_rm_file`` and ``_mv_file``, call the
    wrapped filesystem's, each in a thread, so that several run at once. Its
    ``_mv_file`` is the wrapped one's ``mv``, e.g. a local rename. Its data
    methods call the wrapped one's too, and its files are the wrapped one's,
    awaited through a `SyncFileAdapter`.

    It wins on storages listing one folder per request, such as a local disk
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
        What the storage does. ``StorageProfile()`` if it is unknown.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

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
    ...     storage_profile=StorageProfile(renames_folders=True),
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
            storage_profile=storage_profile,
            **kwargs,
        )
