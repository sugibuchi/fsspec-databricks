# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""What the wrappers share: the filesystem wrapped, its primitives, awaited or run in threads.

`_AsyncFileSystemWrapper` awaits an async filesystem's coroutines on its loop.
`_SyncFileSystemWrapper` runs a sync one's methods, each in a thread. Both
pass the wrapped one's data methods, such as ``_cat_file``, on as they are,
under ``io_limit``. A family's wrapper adds its own primitives.

This module depends on `sync_client` of `common`, `fsspec_async` of `files`,
and `listing` of `tree`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..common.sync_client import SyncClientSupport
from ..files.fsspec_async import SyncFileAdapter
from ..tree.listing import _tree_ils

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from fsspec.asyn import AsyncFileSystem
    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.profile import StorageProfile

__all__: list[str] = []


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
        """The filesystem wrapped, itself unwrapped if a wrapper: a `Call` names it.

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

    async def _ils(
        self, path: str, recursive: bool = False, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        if recursive:
            # fsspec's find gives all at once, in no stated order: one listing per directory.
            async for entry in _tree_ils(self, path, recursive=False, **kwargs):
                yield entry
            return

        # fsspec has no listing in pages: the wrapped one's _ls, all at once.
        for entry in await self.fs._ls(path, detail=True, **kwargs):
            yield entry

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return await self.fs._info(path, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await self.fs._rm_file(path, **kwargs)

    # fsspec's copy and move take paths: the entry's name alone, and no _info
    # first. A subclass overriding _cp_file or _mv_file, e.g. with a rename,
    # is so used by the strategies too.
    async def _cp_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        await self._cp_file(entry["name"], path2, **kwargs)

    async def _mv_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        await self._mv_file(entry["name"], path2, **kwargs)

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


class _SyncFileSystemWrapper(SyncClientSupport, _FileSystemWrapper):
    """The primitives of a sync filesystem, each run in a thread of the wrapper's own.

    There are as many threads as ``operation_concurrency`` and
    ``io_concurrency`` together, when both are numbers. ``operation_limit`` and
    ``io_limit`` bound how many run at once: a filesystem unsafe to call from
    several threads needs both at 1.
    """

    fs: AbstractFileSystem

    def _thread_count(self) -> int | None:
        # The data methods hold no permit of operation_limit: they need threads
        # of their own.
        concurrencies = (self.operation_concurrency, self.io_concurrency)  # type: ignore[attr-defined]
        if all(isinstance(c, int) for c in concurrencies):
            return sum(concurrencies)
        else:
            return None

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        return await self._run_in_thread(self.fs.ls, path, detail=detail, **kwargs)

    async def _ils(
        self, path: str, recursive: bool = False, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        if recursive:
            # fsspec's find gives all at once, in no stated order: one listing per directory.
            async for entry in _tree_ils(self, path, recursive=False, **kwargs):
                yield entry
            return

        # fsspec has no listing in pages: the wrapped one's ls, all at once.
        for entry in await self._run_in_thread(self.fs.ls, path, detail=True, **kwargs):
            yield entry

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return await self._run_in_thread(self.fs.info, path, **kwargs)

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await self._run_in_thread(self.fs.rm_file, path, **kwargs)

    # fsspec's copy and move take paths: the entry's name alone, and no info
    # first. A subclass overriding _cp_file or _mv_file, e.g. with a rename,
    # is so used by the strategies too.
    async def _cp_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        await self._cp_file(entry["name"], path2, **kwargs)

    async def _mv_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        await self._mv_file(entry["name"], path2, **kwargs)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        await self._run_in_thread(self.fs.cp_file, path1, path2, **kwargs)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        # A sync filesystem's own move, e.g. a local rename, is its mv.
        await self._run_in_thread(self.fs.mv, path1, path2, **kwargs)

    async def _rmdir(self, path: str) -> None:
        await self._run_in_thread(self.fs.rmdir, path)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        await self._run_in_thread(self.fs.makedirs, path, exist_ok=exist_ok)

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        await self._run_in_thread(
            self.fs.mkdir, path, create_parents=create_parents, **kwargs
        )

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        async with self.io_limit:
            return await self._run_in_thread(
                self.fs.cat_file, path, start=start, end=end, **kwargs
            )

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        async with self.io_limit:
            await self._run_in_thread(self.fs.pipe_file, path, value, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await self._run_in_thread(self.fs.get_file, rpath, lpath, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        async with self.io_limit:
            await self._run_in_thread(self.fs.put_file, lpath, rpath, **kwargs)

    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        file = await self._run_in_thread(self.fs.open, path, mode, **kwargs)
        return SyncFileAdapter(file)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return self.fs._open(path, mode=mode, **kwargs)
