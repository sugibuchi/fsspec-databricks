# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""An abstract async filesystem whose recursive methods are the tree strategies.

`TreeFileSystem` is for storages with a hierarchical namespace, which list one
folder per request. A subclass implements the storage's primitives, and gets
fsspec's recursive methods from the tree strategies.

This module depends on `cp`, `du`, `find`, `glob`, `mv`, `rm`,
`walk`, and
`filesystem` and `operation` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec.asyn import sync

from fsspec_alt_strategies.monitor import Call

from ..common.filesystem import (
    _answer,
    _BaseFileSystem,
    _copy_errors,
    _du_answer,
    _failed_folder,
    _raise,
    _strip_only_protocol,
    _with,
)
from ..common.operation import _collect_with_monitors, _with_monitors
from .cp import tree_local_cp
from .du import tree_du
from .find import tree_find
from .glob import tree_glob
from .mv import tree_local_mv
from .rm import tree_rm
from .walk import tree_walk

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable

    from fsspec_alt_strategies.profile import StorageProfile


__all__ = ["TreeFileSystem"]


class TreeFileSystem(_BaseFileSystem):
    """An abstract async filesystem, its recursive methods the tree strategies.

    It suits storages with a hierarchical namespace, which list one folder per
    request, such as Azure Data Lake Storage Gen2. It overrides fsspec's
    ``_walk``, ``_find``, ``_du``, ``_glob``, and ``_rm``, ``_copy`` and ``mv``
    when recursive, with `tree_walk`, `tree_find`, `tree_du`, `tree_glob`,
    `tree_rm`, `tree_cp` and `tree_mv`. Their answers are fsspec's, and their
    failures are raised, a single one as it is, several in an ExceptionGroup.
    A folder that fails to list is yielded empty by ``_walk``, as fsspec's
    ``walk`` yields it. ``on_error`` handles every folder's failure, where
    fsspec's applies it to the root only, as its walk recurses without it.

    A subclass implements the storage's primitives, which fsspec leaves
    abstract: ``_info``, ``_rm_file``, ``_rmdir``, ``_cp_file`` and
    ``_makedirs``, and ``_ils``, a folder's listing as an async iterator,
    which ``_ls`` lists with. It implements ``open_async`` too, giving its files
    ``io_limit`` as their concurrency limit, and ``_open``. ``_cat_file``,
    ``_pipe_file``, ``_get_file`` and ``_put_file`` read and write through
    ``open_async``'s files. It may implement ``_rm_tree`` too, a storage's
    recursive delete of a folder, which a recursive ``rm`` then uses.

    Two limits bound its requests, each an `asyncio.Semaphore` of its own:

    - ``operation_limit``, the strategies' requests, such as listings,
      deletions and copies;
    - ``io_limit``, the reads and writes of data, by its files.

    ``storage_profile``, a `StorageProfile`, tells the strategies what the
    storage does. It is required: `StorageProfile()` says the storage is
    unknown, and the strategies then take the way right on any storage.

    A recursive ``mv``, or one of a single file, moves each file with
    ``_mv_file``, which is fsspec's copy then delete. A subclass whose storage renames a path in one request
    overrides it. If its rename moves a folder too, with all in it, the
    profile's ``renames_folders=True`` says so: a recursive ``mv`` then moves
    each folder whole where it can.

    Parameters
    ----------
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The reads and writes of data at a time.
        A semaphore is used as it is, shared with what else holds it.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If a concurrency is less than 1, or ``storage_profile`` is not set.

    See Also
    --------
    FlatFileSystem : The same, for flat storages, such as object stores.

    Examples
    --------
    >>> class MyFileSystem(TreeFileSystem):
    ...     protocol = "my"
    ...     storage_profile = StorageProfile()  # Unknown.
    ...
    ...     async def _ils(self, path, **kwargs): ...
    ...     async def _info(self, path, **kwargs): ...
    ...     async def _rm_file(self, path, **kwargs): ...
    ...     async def _rmdir(self, path): ...
    ...     async def _cp_file(self, path1, path2, **kwargs): ...
    ...     async def _makedirs(self, path, exist_ok=False): ...
    ...
    ...     async def open_async(self, path, mode="rb", **kwargs):
    ...         file = MyFile(self, path, mode, concurrency_limit=self.io_limit)
    ...         return AsyncFileAdapter(file)
    ...
    ...     def _open(self, path, mode="rb", **kwargs):
    ...         return MyFile(self, path, mode, concurrency_limit=self.io_limit)
    >>> fs = MyFileSystem(operation_concurrency=32, io_concurrency=8)
    """

    storage_profile: StorageProfile
    """What the storage does, for the strategies: a subclass must set it."""

    _rm_tree: Callable[[str], Awaitable[None]] | None = None
    """Optional: deletes a folder with everything in it, in one request.

    A subclass whose storage has a recursive delete defines it as
    ``async def _rm_tree(self, path)``. A recursive ``rm`` then deletes each
    folder root by it, and walks only the roots it fails on. Left None, every
    folder is walked.
    """

    def __init__(
        self,
        *args: Any,
        operation_concurrency: int | asyncio.Semaphore,
        io_concurrency: int | asyncio.Semaphore,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *args,
            operation_concurrency=operation_concurrency,
            io_concurrency=io_concurrency,
            **kwargs,
        )

    # What a subclass implements: the storage's primitives, as fsspec names them.

    @abstractmethod
    def _ils(self, path: str, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """List a folder, as an async iterator of its entries, as the strategies list.

        The entries are as fsspec's ``_ls`` gives them with ``detail``: a
        storage listing a folder in pages yields each page's as it comes. A
        file yields itself, as fsspec's ``_ls`` lists it.
        """

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        """List a folder, as fsspec's ``_ls`` does: all of ``_ils``'s entries, or their names."""
        entries = [entry async for entry in self._ils(path, **kwargs)]
        return entries if detail else [e["name"] for e in entries]

    @abstractmethod
    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Describe a path, raising FileNotFoundError if there is none."""

    @abstractmethod
    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file.

        The strategies call it only on paths they found to be files, by a
        listing or an ``info``: it need not check that the path is no folder.
        fsspec's own methods, e.g. ``rm`` when not recursive, may not.
        """

    @abstractmethod
    async def _rmdir(self, path: str) -> None:
        """Delete an empty folder."""

    @abstractmethod
    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file within the storage."""

    @abstractmethod
    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create a folder and its parents, raising FileExistsError unless ``exist_ok``."""

    @abstractmethod
    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file whose methods are awaited, as fsspec's ``open_async`` does.

        The file holds ``io_limit`` as its concurrency limit, e.g. an
        `AsyncFileAdapter` of one of this package's file classes.
        """

    @abstractmethod
    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file, as fsspec's ``_open`` does, for the sync ``open``."""

    async def _walk(
        self,
        path: str,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: str | Callable[[Exception], Any] = "omit",
        **kwargs: Any,
    ) -> AsyncGenerator[tuple[str, Any, Any]]:
        detail = kwargs.pop("detail", False)
        root = self._strip_protocol(path)
        walk = tree_walk(
            root,
            ils=_with(self._ils, kwargs),
            maxdepth=maxdepth,
            topdown=topdown,
            # The failed folders' errors, which fsspec's on_error then handles:
            # its "omit" still yields them empty.
            on_error="raise" if on_error == "raise" else None,
            concurrency_limit=self.operation_limit,
        )

        async with aclosing(
            _with_monitors(walk, Call(self._unwrapped(), "walk", path))
        ) as items:
            try:
                first = await anext(items)
            except StopAsyncIteration:
                return
            except NotADirectoryError:
                # fsspec's walk of a file yields the file, under no name.
                entry = await self._info(root)
                yield (root, {}, {"": entry}) if detail else (root, [], [""])
                return
            except OSError as e:
                yield _failed_folder(root, e, on_error, detail)
                return

            item = first
            while True:
                if isinstance(item, Exception):
                    yield _failed_folder(item.folder, item, on_error, detail)  # type: ignore[attr-defined]
                else:
                    folder, dirs, files = item
                    if not detail:
                        dirs, files = list(dirs), list(files)
                    yield folder, dirs, files

                try:
                    item = await anext(items)
                except StopAsyncIteration:
                    return

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> Any:
        detail = kwargs.pop("detail", False)
        entries, _ = await _collect_with_monitors(
            tree_find(
                self._strip_protocol(path),
                ils=_with(self._ils, kwargs),
                info=self._info,
                maxdepth=maxdepth,
                withdirs=withdirs,
                concurrency_limit=self.operation_limit,
            ),
            Call(self._unwrapped(), "find", path),
        )

        return _answer(entries, detail)

    async def _du(
        self, path: str, total: bool = True, maxdepth: int | None = None, **kwargs: Any
    ) -> Any:
        if kwargs.pop("withdirs", False):
            return await super()._du(
                path, total=total, maxdepth=maxdepth, withdirs=True, **kwargs
            )

        op = tree_du(
            self._strip_protocol(path),
            ils=_with(self._ils, kwargs),
            info=self._info,
            maxdepth=maxdepth,
            concurrency_limit=self.operation_limit,
        )
        entries, _ = await _collect_with_monitors(
            op, Call(self._unwrapped(), "du", path)
        )

        return _du_answer(op.state.size, entries, total)

    async def _glob(self, path: str, maxdepth: int | None = None, **kwargs: Any) -> Any:
        detail = kwargs.pop("detail", False)
        withdirs = kwargs.pop("withdirs", True)
        entries, _ = await _collect_with_monitors(
            tree_glob(
                _strip_only_protocol(self, path),
                ils=_with(self._ils, kwargs),
                info=self._info,
                maxdepth=maxdepth,
                withdirs=withdirs,
                concurrency_limit=self.operation_limit,
            ),
            Call(self._unwrapped(), "glob", path),
        )

        return _answer(entries, detail)

    async def _rm(
        self,
        path: str | list[str],
        recursive: bool = False,
        batch_size: int | None = None,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        if not recursive:
            await super()._rm(path, recursive, batch_size, maxdepth, **kwargs)
            return

        _, errors = await _collect_with_monitors(
            tree_rm(
                _strip_only_protocol(self, path),
                ils=self._ils,
                info=self._info,
                rm_file=self._rm_file,
                rm_folder=self._rmdir,
                rm_tree=self._rm_tree,
                profile=self.storage_profile,
                maxdepth=maxdepth,
                concurrency_limit=self.operation_limit,
            ),
            Call(self._unwrapped(), "rm", path),
            kwargs.get("callback"),
        )

        _raise(errors, "Deleting")

    async def _copy(
        self,
        path1: str | list[str],
        path2: str | list[str],
        recursive: bool = False,
        on_error: str | None = None,
        maxdepth: int | None = None,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        if not recursive or not isinstance(path2, str):
            await super()._copy(
                path1, path2, recursive, on_error, maxdepth, batch_size, **kwargs
            )
            return

        _, errors = await _collect_with_monitors(
            tree_local_cp(
                _strip_only_protocol(self, path1),
                _strip_only_protocol(self, path2),
                ils=self._ils,
                info=self._info,
                cp_file=self._cp_file,
                makedirs=self._makedirs,
                rm_file=self._rm_file,
                profile=self.storage_profile,
                maxdepth=maxdepth,
                concurrency_limit=self.operation_limit,
            ),
            Call(self._unwrapped(), "copy", path1, path2),
            kwargs.get("callback"),
        )

        _raise(_copy_errors(errors, on_error), "Copying")

    async def _mv(
        self,
        path1: str | list[str],
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Move files, as fsspec's ``mv`` does: with `tree_mv` when recursive.

        A single file goes by `tree_mv` too, recursive or not, so that it moves
        by ``_mv_file``, the storage's rename where a subclass overrides it.
        """
        if path1 == path2:
            return

        if not recursive and not await self._single_file(path1):
            # fsspec's own: a copy, then the deletion of its sources.
            await self._copy(path1, path2, recursive, "raise", maxdepth, **kwargs)
            await self._rm(path1, recursive=recursive)
            return

        _, errors = await _collect_with_monitors(
            tree_local_mv(
                _strip_only_protocol(self, path1),
                _strip_only_protocol(self, path2),
                ils=self._ils,
                info=self._info,
                mv_file=self._mv_file,
                makedirs=self._makedirs,
                rm_folder=self._rmdir,
                profile=self.storage_profile,
                maxdepth=maxdepth,
                concurrency_limit=self.operation_limit,
            ),
            Call(self._unwrapped(), "mv", path1, path2),
            kwargs.get("callback"),
        )

        _raise(errors, "Moving")

    # fsspec makes sync methods only from the async ones it knows: _mv and _rmdir are
    # not. Each awaits the instance's own, so that a subclass's override is the one run.

    def mv(self, path1: str | list[str], path2: str, **kwargs: Any) -> None:
        return sync(self.loop, self._mv, path1, path2, **kwargs)

    def rmdir(self, path: str) -> None:
        return sync(self.loop, self._rmdir, path)
