# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Existing filesystems, wrapped as a `TreeFileSystem` or a `FlatFileSystem`.

A wrapper is a family's filesystem class whose primitives are another
filesystem's, so that its recursive methods are the family's strategies:

- `AsyncTreeWrapper` and `SyncTreeWrapper` wrap an async or a sync filesystem
  as a `TreeFileSystem`;
- `AsyncFlatWrapper` and `SyncFlatWrapper` wrap one as a `FlatFileSystem`.

An async filesystem's coroutines are awaited on its loop. A sync one's methods
each run in a thread. Both pass the wrapped one's data methods, such as
``_cat_file``, on as they are, under ``io_limit``.

A wrapper is light, and not cached, as fsspec caches filesystems: make one on
demand, e.g. to watch a call with a monitor, and drop it after. The developer
chooses the wrapper: none is chosen for a filesystem.

fsspec has no listing by prefix, which the flat strategies list with. A flat
wrapper's ``_ils`` lists with the wrapped filesystem's ``find`` of the
prefix's folder, which object stores answer in one paginated listing. That
costs twice:

- ``find`` returns the whole listing at once, so the strategies start only
  once all of it has arrived, and it is held in memory, not streamed.
- It lists the prefix's whole folder: for "data/2024-", all of "data/", the
  keys not starting with the prefix then left out.

A subclass overriding ``_ils`` with the storage's own listing by prefix, e.g.
from its SDK, streams it page by page, and lists only the prefix.

This module depends on `filesystem` of `tree` and of `flat`, and on
`files.fsspec_async`.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from .files.fsspec_async import SyncFileAdapter
from .flat.filesystem import FlatFileSystem
from .tree.filesystem import TreeFileSystem

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.profile import StorageProfile

__all__ = ["AsyncFlatWrapper", "AsyncTreeWrapper", "SyncFlatWrapper", "SyncTreeWrapper"]


class _FileSystemWrapper:
    """The filesystem wrapped, and its paths, as the wrapper's own."""

    # Made on demand, and dropped after: fsspec keeps none.
    cachable = False
    io_limit: asyncio.Semaphore

    def __init__(
        self,
        fs: Any,
        *,
        storage_profile: StorageProfile,
        **kwargs: Any,
    ) -> None:
        self.fs = fs
        # A wrapper's storage is the one it wraps: its profile comes with it.
        self.storage_profile = storage_profile
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} of {self.fs!r}>"

    def _strip_protocol(self, path: Any) -> Any:  # type: ignore[override]
        # The wrapped filesystem's paths, not this class's protocol.
        return self.fs._strip_protocol(path)

    def unstrip_protocol(self, name: str) -> str:
        return self.fs.unstrip_protocol(name)

    # The sync methods fsspec leaves to each filesystem, the wrapped one's as
    # they are: a router passes them on to its storages.

    def created(self, path: str) -> Any:
        return self.fs.created(path)

    def modified(self, path: str) -> Any:
        return self.fs.modified(path)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> str:
        return self.fs.sign(path, expiration=expiration, **kwargs)

    def invalidate_cache(self, path: str | None = None) -> None:
        self.fs.invalidate_cache(path)

    def _unwrapped(self) -> Any:
        """The filesystem wrapped, unwrapped too if a wrapper: a monitor's `Call` names it.

        A wrapper is made on demand, and dropped after: the filesystem it wraps
        is the one its callers know.
        """
        unwrap = getattr(self.fs, "_unwrapped", None)
        return unwrap() if unwrap is not None else self.fs


class _AsyncFileSystemWrapper(_FileSystemWrapper):
    """The primitives of an async filesystem, awaited on its loop."""

    fs: AsyncFileSystem

    def __init__(self, fs: AsyncFileSystem, **kwargs: Any) -> None:
        # Its coroutines run on its loop, so the wrapper's do too.
        kwargs.setdefault("asynchronous", fs.asynchronous)
        if not fs.asynchronous:
            kwargs.setdefault("loop", fs.loop)
        super().__init__(fs, **kwargs)

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        return await self.fs._ls(path, detail=detail, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return await self.fs._info(path, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await self.fs._rm_file(path, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        await self.fs._cp_file(path1, path2, **kwargs)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        await self.fs._mv_file(path1, path2, **kwargs)

    async def _rmdir(self, path: str) -> None:
        await self.fs._rmdir(path)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        await self.fs._makedirs(path, exist_ok=exist_ok)

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        await self.fs._mkdir(path, create_parents=create_parents, **kwargs)

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        async with self.io_limit:
            return await self.fs._cat_file(path, start=start, end=end, **kwargs)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        async with self.io_limit:
            await self.fs._pipe_file(path, value, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await self.fs._get_file(rpath, lpath, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await self.fs._put_file(lpath, rpath, **kwargs)

    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return await self.fs.open_async(path, mode, **kwargs)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return self.fs._open(path, mode=mode, **kwargs)


class _SyncFileSystemWrapper(_FileSystemWrapper):
    """The primitives of a sync filesystem, each run in a thread.

    The threads are those of the loop's default executor. ``operation_limit``
    and ``io_limit`` bound how many run at once for the wrapper: a filesystem
    unsafe to call from several threads needs them at 1.
    """

    fs: AbstractFileSystem

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self.fs.ls, path, detail=detail, **kwargs)

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self.fs.info, path, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await asyncio.to_thread(self.fs.rm_file, path, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        await asyncio.to_thread(self.fs.cp_file, path1, path2, **kwargs)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        # A sync filesystem's own move, e.g. a local rename, is its mv.
        await asyncio.to_thread(self.fs.mv, path1, path2, **kwargs)

    async def _rmdir(self, path: str) -> None:
        await asyncio.to_thread(self.fs.rmdir, path)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        await asyncio.to_thread(self.fs.makedirs, path, exist_ok=exist_ok)

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        await asyncio.to_thread(
            self.fs.mkdir, path, create_parents=create_parents, **kwargs
        )

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        async with self.io_limit:
            return await asyncio.to_thread(
                self.fs.cat_file, path, start=start, end=end, **kwargs
            )

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        async with self.io_limit:
            await asyncio.to_thread(self.fs.pipe_file, path, value, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await asyncio.to_thread(self.fs.get_file, rpath, lpath, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await asyncio.to_thread(self.fs.put_file, lpath, rpath, **kwargs)

    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        file = await asyncio.to_thread(self.fs.open, path, mode, **kwargs)
        return SyncFileAdapter(file)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return self.fs._open(path, mode=mode, **kwargs)


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
            # A wrapper is a tree filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="tree"),
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
            # A wrapper is a tree filesystem, whatever it wraps.
            storage_profile=replace(storage_profile, fs_type="tree"),
            **kwargs,
        )


class AsyncFlatWrapper(_AsyncFileSystemWrapper, FlatFileSystem):
    """An async filesystem, as a `FlatFileSystem`: its recursive methods the flat strategies.

    Its primitives, such as ``_info``, ``_cp_file`` and ``_mv_file``, are the
    wrapped filesystem's, awaited on its loop. So are its data methods, such
    as ``_cat_file``, and its files. The wrapper runs on the same loop, sync or
    not as the wrapped one is.

    Its ``_ils`` lists with the wrapped one's ``_find`` of the prefix's folder,
    and its ``_rm_files`` deletes with its ``_rm``, as s3fs deletes a batch in
    one request. It wins on object stores, as `FlatFileSystem` does. It is not
    cached: each is new, and may be dropped once used.

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

    async def _ils(self, prefix: str) -> AsyncIterator[dict[str, Any]]:
        found = await self.fs._find(prefix.rpartition("/")[0], detail=True)
        for entry in _under(found, prefix):
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

    Its ``_ils`` lists with the wrapped one's ``find`` of the prefix's folder,
    and its ``_rm_files`` deletes with its ``rm``. A filesystem unsafe to call
    from several threads needs both concurrencies at 1. It is not cached: each
    is new, and may be dropped once used.

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

    async def _ils(self, prefix: str) -> AsyncIterator[dict[str, Any]]:
        found = await asyncio.to_thread(
            self.fs.find, prefix.rpartition("/")[0], detail=True
        )
        for entry in _under(found, prefix):
            yield entry

    async def _rm_files(self, paths: list[str]) -> None:
        try:
            await asyncio.to_thread(self.fs.rm, list(paths))
        except FileNotFoundError:
            # A key gone meanwhile: the rest one by one, a missing key no error.
            await asyncio.gather(*(self._rm_if_there(p) for p in paths))

    async def _rm_if_there(self, path: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            await asyncio.to_thread(self.fs.rm_file, path)


def _under(found: dict[str, dict[str, Any]], prefix: str) -> list[dict[str, Any]]:
    """The entries of a ``find`` starting with ``prefix``, as ``ils`` yields them."""
    return [e for name, e in found.items() if name.startswith(prefix)]
