# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""An abstract async filesystem whose recursive methods are the flat strategies.

`FlatFileSystem` is for flat storages, such as object stores, which list every
key under a prefix in one paginated request. A subclass implements the
storage's primitives, that listing among them, and gets fsspec's recursive
methods from the flat strategies.

This module depends on `cp`, `du`, `find`, `glob`, `mv`, `rm`, `walk`, and
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
    _failed_dir,
    _raise,
    _strip_only_protocol,
)
from ..common.operation import _collect_with_monitors, _with_monitors
from .cp import _flat_cp
from .du import _flat_du
from .find import _flat_find
from .glob import _flat_glob
from .mv import _flat_mv
from .rm import _flat_rm
from .walk import _flat_walk

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, AsyncIterator, Callable

    from fsspec_alt_strategies.profile import StorageProfile


__all__ = ["FlatFileSystem"]


class FlatFileSystem(_BaseFileSystem):
    """An abstract async filesystem, its recursive methods the flat strategies.

    It suits flat storages, such as object stores, which list every key under a
    prefix in one paginated request. It overrides fsspec's ``_walk``,
    ``_find``, ``_du``, ``_glob``, and ``_rm``, ``_copy`` and ``_mv`` when
    recursive, with the flat strategies. These list every key under a prefix
    with a recursive ``_ils``, and act on each key as it arrives. Their answers are
    fsspec's, and their failures are raised, a single one as it is, several in
    an ExceptionGroup.

    A subclass implements the storage's primitives:

    - ``_ils``, a directory's listing, which ``_ls`` and so fsspec's own
      methods list with, and with ``recursive=True`` every key below it, which
      the strategies list with;
    - ``_rm_files``, a deletion of several files in one request, with
      ``rm_batch_size``;
    - ``_info`` and ``_rm_file``;
    - ``_cp_file_entry`` and ``_mv_file_entry``, a copy and a move of a key by its entry,
      as its listing gave it, e.g. with its size. fsspec's ``_cp_file`` and
      ``_mv_file`` ask the entry, then call them;
    - ``open_async``, giving its files ``io_limit`` as their concurrency
      limit, and ``_open``.

    The data methods, ``_cat_file``, ``_pipe_file``, ``_get_file`` and
    ``_put_file``, read and write through ``open_async``'s files.

    Two limits bound its requests, each an `asyncio.Semaphore` of its own:

    - ``operation_limit``, the strategies' requests, such as deletions and
      copies, but not their listings, which come one page after another;
    - ``io_limit``, the reads and writes of data, by its files.

    ``storage_profile``, a `StorageProfile`, tells the strategies what the
    storage does. With ``renames_files=True``, a recursive ``mv``, or one of a
    single file, moves each key by ``_mv_file_entry``, the storage's rename.
    Otherwise it copies each key, and deletes them in batches.

    Parameters
    ----------
    operation_concurrency : int or asyncio.Semaphore
        The requests of the strategies at a time.
        A semaphore is used as it is, shared with what else holds it.
    io_concurrency : int or asyncio.Semaphore
        The reads and writes of data at a time.
        A semaphore is used as it is, shared with what else holds it. Each
        holds a block or a part, so it bounds their memory too: at most this
        many times the file class's ``max_block_size`` in flight.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If a concurrency is less than 1, or ``rm_batch_size`` is not set. Also
        if ``storage_profile`` is not set while ``_load_storage_profile`` is
        not overridden.

    See Also
    --------
    TreeFileSystem : The same, for storages with a hierarchical namespace.

    Examples
    --------
    >>> class MyFileSystem(FlatFileSystem):
    ...     protocol = "my"
    ...     storage_profile = StorageProfile()  # Unknown.
    ...     rm_batch_size = 1000  # As S3's DeleteObjects takes.
    ...
    ...     async def _ils(self, path, recursive=False, prefix="", **kwargs): ...
    ...     async def _rm_files(self, paths): ...
    ...     async def _info(self, path, **kwargs): ...
    ...     async def _rm_file(self, path, **kwargs): ...
    ...     async def _cp_file_entry(self, entry, path2, **kwargs): ...
    ...     async def _mv_file_entry(self, entry, path2, **kwargs): ...
    ...
    ...     async def open_async(self, path, mode="rb", **kwargs):
    ...         file = MyFile(self, path, mode, concurrency_limit=self.io_limit)
    ...         return AsyncFileAdapter(file)
    ...
    ...     def _open(self, path, mode="rb", **kwargs):
    ...         return MyFile(self, path, mode, concurrency_limit=self.io_limit)
    >>> fs = MyFileSystem(operation_concurrency=32, io_concurrency=8)
    """

    rm_batch_size: int
    """The most keys ``_rm_files`` deletes in one request, e.g. 1000 on S3."""

    storage_profile: StorageProfile
    """What the storage does, for the strategies.

    A subclass sets it, or overrides ``_load_storage_profile`` instead.
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

        size = getattr(self, "rm_batch_size", None)
        if not isinstance(size, int) or size < 1:
            raise ValueError(f"rm_batch_size must be set, to at least 1: {size}")

    # What a subclass implements: the storage's primitives.

    @abstractmethod
    def _ils(
        self, path: str, recursive: bool = False, prefix: str = "", **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """List a directory, or every key below it, as an async iterator of entries.

        The entries are as fsspec's ``_ls`` gives them with ``detail``, a key
        ending with "/" being a directory's marker. A storage listing in pages
        yields each page's as it comes. ``prefix`` keeps the names starting
        with ``path/prefix``, or ``prefix`` at the root: object stores list so
        in one request, with a delimiter or without.

        Without ``recursive``, it lists ``path``'s files and subdirectories,
        e.g. by an object store's listing with the delimiter "/". A file yields
        itself, as fsspec's ``_ls`` lists it, and a missing ``path`` raises
        FileNotFoundError. fsspec's own methods list so, through ``_ls``.

        With ``recursive=True``, it lists every key below ``path``, at any
        depth, with no directory but markers: the strategies list so, e.g.
        "data" with the prefix "2024-" for the glob "data/2024-*". Nothing
        below ``path``, or ``path`` missing, lists nothing: the strategies
        then ask ``_info``. The keys come in order of their whole names, as
        object stores list them, across buckets too at the root: the
        strategies count on it to know when a directory is done. Each key
        comes once: a key listed twice is copied, moved, deleted or counted
        twice.
        """

    @abstractmethod
    async def _rm_files(self, paths: list[str]) -> None:
        """Delete several files in one request, e.g. with S3's DeleteObjects.

        A key missing is no error. It raises if any is not deleted. The
        strategies give it only keys they listed, or found by an ``info``: it
        need not check them again.
        """

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        """List a directory, as fsspec's ``_ls`` does: all of ``_ils``'s entries, or their names."""
        entries = [entry async for entry in self._ils(path, **kwargs)]
        return entries if detail else [e["name"] for e in entries]

    @abstractmethod
    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Describe a path, raising FileNotFoundError if there is none."""

    @abstractmethod
    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        """Delete a file, as fsspec's ``_rm`` does when not recursive.

        The flat strategies never call it: they delete with ``_rm_files``.
        fsspec's own methods do, e.g. ``rm`` when not recursive, on any path.
        """

    @abstractmethod
    async def _cp_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Copy a key within the storage, by its entry, to ``path2``.

        ``entry`` is the source's, as ``_ils`` or ``_info`` gave it, its
        ``"name"`` the source key. Its other keys, e.g. the size, may save a
        request, e.g. one telling whether a copy goes in parts. They may be
        stale, listed before the copy: a copy must still succeed, or fail as
        the storage says, if the key changed since.
        """

    @abstractmethod
    async def _mv_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Move a key within the storage, by its entry, to ``path2``.

        ``entry`` is as `_cp_file_entry` takes it. The flat strategies call it only
        where the profile's ``renames_files=True`` says the storage renames: a
        directory's marker too, its name ending with "/". One with no rename
        copies, then deletes the source, for fsspec's ``_mv_file``.
        """

    @abstractmethod
    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file whose methods are awaited, as fsspec's ``open_async`` does.

        The file holds ``io_limit`` as its concurrency limit, e.g. an
        `AsyncFileAdapter` of one of this package's file classes.
        """

    @abstractmethod
    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file, as fsspec's ``_open`` does, for the sync ``open``."""

    # fsspec's recursive methods, answered by the flat strategies.

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
        walk = _flat_walk(self, root, maxdepth=maxdepth, topdown=topdown)

        async with aclosing(
            _with_monitors(walk, Call(self._unwrapped(), "walk", path))
        ) as directories:
            try:
                async for item in directories:
                    directory, dirs, files = item
                    if not detail:
                        dirs, files = list(dirs), list(files)
                    yield directory, dirs, files
            except NotADirectoryError:
                # fsspec's walk of a file yields the file, under no name.
                entry = await self._info(root)
                yield (root, {}, {"": entry}) if detail else (root, [], [""])
            except OSError as e:
                # The listing's error ends the walk: fsspec's on_error handles
                # it as a failure to list the root.
                yield _failed_dir(root, e, on_error, detail)

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> Any:
        detail = kwargs.pop("detail", False)
        entries, _ = await _collect_with_monitors(
            _flat_find(
                self,
                self._strip_protocol(path),
                maxdepth=maxdepth,
                withdirs=withdirs,
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

        op = _flat_du(self, self._strip_protocol(path), maxdepth=maxdepth)

        entries, _ = await _collect_with_monitors(
            op, Call(self._unwrapped(), "du", path)
        )
        return _du_answer(op.state.size, entries, total)

    async def _glob(self, path: str, maxdepth: int | None = None, **kwargs: Any) -> Any:
        detail = kwargs.pop("detail", False)
        entries, _ = await _collect_with_monitors(
            _flat_glob(
                self,
                _strip_only_protocol(self, path),
                maxdepth=maxdepth,
                withdirs=kwargs.pop("withdirs", True),
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
            _flat_rm(self, _strip_only_protocol(self, path), maxdepth=maxdepth),
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
            _flat_cp(
                self,
                self,
                _strip_only_protocol(self, path1),
                _strip_only_protocol(self, path2),
                maxdepth=maxdepth,
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
        """Move files, as fsspec's ``mv`` does: with the flat move when recursive.

        A single file goes by the flat move too, recursive or not, so that it
        moves by ``_mv_file_entry`` where the profile's ``renames_files`` says it
        renames.
        """
        if path1 == path2:
            return

        if not recursive and not await self._single_file(path1):
            # fsspec's own: a copy, then the deletion of its sources.
            await self._copy(path1, path2, recursive, "raise", maxdepth, **kwargs)
            await self._rm(path1, recursive=recursive)
            return

        _, errors = await _collect_with_monitors(
            _flat_mv(
                self,
                self,
                _strip_only_protocol(self, path1),
                _strip_only_protocol(self, path2),
                maxdepth=maxdepth,
            ),
            Call(self._unwrapped(), "mv", path1, path2),
            kwargs.get("callback"),
        )
        _raise(errors, "Moving")

    # fsspec makes sync methods only from the async ones it knows: _mv is not. It awaits
    # the instance's own, so that a subclass's override is the one run.

    def mv(self, path1: str | list[str], path2: str, **kwargs: Any) -> None:
        return sync(self.loop, self._mv, path1, path2, **kwargs)
