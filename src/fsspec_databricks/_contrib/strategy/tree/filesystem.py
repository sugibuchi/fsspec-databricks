# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""An abstract async filesystem whose recursive methods are the tree strategies.

`TreeFileSystem` is for storages with a hierarchical namespace, which list by
directory. A subclass implements the storage's primitives, and gets fsspec's
recursive methods from the tree strategies.

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
from .cp import _tree_cp
from .du import _tree_du
from .find import _tree_find
from .glob import _tree_glob
from .mv import _tree_mv
from .rm import _tree_rm
from .walk import _tree_walk

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, AsyncIterator, Callable

    from fsspec_alt_strategies.profile import StorageProfile


__all__ = ["TreeFileSystem"]


class TreeFileSystem(_BaseFileSystem):
    """An abstract async filesystem, its recursive methods the tree strategies.

    It suits storages with a hierarchical namespace, such as Azure Data Lake
    Storage Gen2. It overrides fsspec's ``_walk``, ``_find``, ``_du``,
    ``_glob``, and ``_rm``, ``_copy`` and ``_mv`` when recursive, with the tree
    strategies. These list a whole tree by one recursive listing, and a tree
    they prune a directory per request, several at a time. They act on each
    part of a listing as it arrives. Their answers are fsspec's, and their
    failures are raised, a single one as it is, several in an ExceptionGroup.

    A directory that fails to list is yielded empty by ``_walk``, as fsspec's
    ``walk`` yields it. ``on_error`` handles every directory's failure, where
    fsspec's applies it to the root only, as its walk recurses without it.

    A subclass implements the storage's primitives:

    - ``_ils``, a directory's listing, which ``_ls`` and so fsspec's own
      methods list with, and with ``recursive=True`` all below it, which the
      strategies list with;
    - ``_info``, ``_rm_file``, ``_rmdir`` and ``_makedirs``;
    - ``_cp_file_entry`` and ``_mv_file_entry``, a copy and a move of a file
      by its entry, as its listing gave it, e.g. with its size. fsspec's
      ``_cp_file`` and ``_mv_file`` ask the entry, then call them;
    - ``open_async``, giving its files ``io_limit`` as their concurrency
      limit, and ``_open``.

    The data methods, ``_cat_file``, ``_pipe_file``, ``_get_file`` and
    ``_put_file``, read and write through ``open_async``'s files. A subclass
    may implement two requests on a directory whole, used where the profile
    declares them:

    - ``_rm_tree``, a storage's recursive delete, which a recursive ``rm``
      uses where ``deletes_directories=True``;
    - ``_mv_tree``, a storage's rename of a directory, which a recursive
      ``mv`` uses where ``renames_directories=True``.

    Two limits bound its requests, each an `asyncio.Semaphore` of its own:

    - ``operation_limit``, the strategies' requests, such as listings,
      deletions and copies;
    - ``io_limit``, the reads and writes of data, by its files.

    ``storage_profile``, a `StorageProfile`, tells the strategies what the
    storage does. It is required, unless ``_load_storage_profile`` is
    overridden to find it with a request. `StorageProfile()` says the storage
    is unknown, and the strategies then take the way right on any storage.

    A recursive ``mv``, or one of a single file, moves each file with
    ``_mv_file_entry``: the storage's rename, or a copy then a delete where it
    has none. The strategies call it and ``_mv_tree`` only where the
    destination's parent exists, having made it, as a rename need not make it.
    A caller outside the strategies makes it too.

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
        If a concurrency is less than 1, or ``storage_profile`` is not set
        while ``_load_storage_profile`` is not overridden.

    See Also
    --------
    FlatFileSystem : The same, for flat storages, such as object stores.

    Examples
    --------
    >>> class MyFileSystem(TreeFileSystem):
    ...     protocol = "my"
    ...     storage_profile = StorageProfile()  # Unknown.
    ...
    ...     async def _ils(self, path, recursive=False, **kwargs): ...
    ...     async def _info(self, path, **kwargs): ...
    ...     async def _rm_file(self, path, **kwargs): ...
    ...     async def _rmdir(self, path): ...
    ...     async def _cp_file_entry(self, entry, path2, **kwargs): ...
    ...     async def _mv_file_entry(self, entry, path2, **kwargs): ...
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

    # What a subclass implements: the storage's primitives.

    @abstractmethod
    def _ils(
        self, path: str, recursive: bool = False, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """List a directory, or all below it, as an async iterator of entries.

        The entries are as fsspec's ``_ls`` gives them with ``detail``: a
        storage listing a directory in pages yields each page's as it comes. A
        file yields itself, as fsspec's ``_ls`` lists it. They come in any
        order: a directory's listing is done when it ends.

        Each entry is a dict with at least:

        - ``"name"``, the full path, without the protocol, a trailing "/"
          being ignored;
        - ``"type"``, "directory" or "file";
        - ``"size"``, in bytes, 0 for a directory.

        Other keys, such as a modification time, are passed on as they are.
        A missing ``path`` raises FileNotFoundError, when the iteration starts.

        With ``recursive=True``, it lists all below ``path``, at any depth. A
        directory may be left out, as object stores list only files: the
        strategies tell it from the paths below it. The entries come in one of
        two orders, as the strategies count on it to know when a directory is
        done:

        - depth first: a directory's entry, then all below it, before any other
          entry. Its entries among themselves may come in any order.
        - name order: sorted by the whole name, as object stores list keys.
          A directory's entry may then come apart from what is below it, as
          "a/b-x" comes between "a/b" and "a/b/c".

        A storage with no recursive listing of its own, or one breaking these
        orders, answers with ``_tree_ils`` of ``tree/listing.py``. It lists
        depth first, by this ``_ils`` without ``recursive``, one directory per
        request, several at a time::

            from .listing import _tree_ils

            def _ils(self, path, recursive=False, **kwargs):
                if recursive:
                    return _tree_ils(self, path, recursive=False, **kwargs)
                else:
                    ...  # List the entries in path, e.g. a page at a time.

        ``_tree_ils``'s listings ahead hold permits of ``operation_limit`` of
        their own, beside the one held by the caller of ``_ils``. A directory
        that fails to list ends the whole listing, with its error.
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
        """Delete a file.

        The strategies call it only on paths they found to be files, by a
        listing or an ``info``: it need not check that the path is no directory.
        fsspec's own methods, e.g. ``rm`` when not recursive, may not.
        """

    @abstractmethod
    async def _rmdir(self, path: str) -> None:
        """Delete an empty directory.

        A directory already gone raises FileNotFoundError, which the
        strategies count as deleted: on a storage whose directories go with
        their last file, it is. The profile's ``implicit_directories=True``
        saves those requests.
        """

    async def _rm_tree(self, path: str) -> None:
        """Delete a directory with everything in it, in one request.

        Optional: a subclass whose storage has a recursive delete implements
        it, and its profile's ``deletes_directories=True`` says so. A recursive
        ``rm`` then deletes each root by it, with no walk. It is called only
        then. It takes a file too, which it deletes as ``_rm_file`` does, as a
        root may be either. A missing path raises FileNotFoundError, as a walk
        would find it: a storage whose recursive delete succeeds on a missing
        path asks first.
        """
        raise NotImplementedError(f"{type(self).__name__} deletes no directory whole")

    @abstractmethod
    async def _cp_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Copy a file within the storage, by its entry, to ``path2``.

        ``entry`` is the source's, as ``_ils`` or ``_info`` gave it, its
        ``"name"`` the source path. Its other keys, e.g. the size, may save a
        request, e.g. one telling whether a copy goes in parts. They may be
        stale, listed before the copy: a copy must still succeed, or fail as
        the storage says, if the file changed since.
        """

    @abstractmethod
    async def _mv_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Move a file within the storage, by its entry, to ``path2``.

        ``entry`` is as `_cp_file_entry` takes it. A storage renaming a file in
        one request renames it. One with no rename copies, then deletes the
        source. A directory moved whole goes by `_mv_tree`.
        """

    async def _mv_tree(self, path1: str, path2: str) -> None:
        """Move a directory with everything in it to ``path2``, in one request.

        Optional: a subclass whose storage renames a directory implements it,
        and its profile's ``renames_directories=True`` says so. A recursive
        ``mv`` then moves each root by it where it can, with no walk. It is
        called only then, with ``path2`` missing and its parent existing,
        having made it: a rename need not make it. It takes a file too, which
        it moves as ``_mv_file_entry`` does, as a root may be either. A missing
        ``path1`` raises FileNotFoundError.
        """
        raise NotImplementedError(f"{type(self).__name__} moves no directory whole")

    @abstractmethod
    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create a directory and its parents, raising FileExistsError unless ``exist_ok``."""

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        """Create a directory, as fsspec's local ``mkdir`` does, by ``_makedirs``.

        fsspec's own does nothing. An existing path raises FileExistsError.
        Without ``create_parents``, a missing parent raises FileNotFoundError,
        and one that is a file NotADirectoryError. With it, ``_makedirs``
        makes the parents, and answers for a file among them.
        """
        path = self._strip_protocol(path)
        if await self._exists(path):
            raise FileExistsError(path)
        parent = self._parent(path)
        if not create_parents and parent:
            try:
                kind = (await self._info(parent))["type"]
            except FileNotFoundError:
                raise FileNotFoundError(parent) from None
            if kind != "directory":
                raise NotADirectoryError(parent)
        await self._makedirs(path, exist_ok=True)

    @abstractmethod
    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file whose methods are awaited, as fsspec's ``open_async`` does.

        The file holds ``io_limit`` as its concurrency limit, e.g. an
        `AsyncFileAdapter` of one of this package's file classes.
        """

    @abstractmethod
    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Open a file, as fsspec's ``_open`` does, for the sync ``open``."""

    # fsspec's recursive methods, answered by the tree strategies.

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
        walk = _tree_walk(
            self,
            root,
            maxdepth=maxdepth,
            topdown=topdown,
            # The failed directories' errors, which fsspec's on_error then handles:
            # its "omit" still yields them empty.
            on_error="raise" if on_error == "raise" else None,
            ils_kwargs=kwargs,
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
                yield _failed_dir(root, e, on_error, detail)
                return

            item = first
            while True:
                if isinstance(item, Exception):
                    yield _failed_dir(item.directory, item, on_error, detail)  # type: ignore[attr-defined]
                else:
                    directory, dirs, files = item
                    if not detail:
                        dirs, files = list(dirs), list(files)
                    yield directory, dirs, files

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
            _tree_find(
                self,
                self._strip_protocol(path),
                maxdepth=maxdepth,
                withdirs=withdirs,
                ils_kwargs=kwargs,
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

        op = _tree_du(
            self, self._strip_protocol(path), maxdepth=maxdepth, ils_kwargs=kwargs
        )
        entries, _ = await _collect_with_monitors(
            op, Call(self._unwrapped(), "du", path)
        )

        return _du_answer(op.state.size, entries, total)

    async def _glob(self, path: str, maxdepth: int | None = None, **kwargs: Any) -> Any:
        detail = kwargs.pop("detail", False)
        withdirs = kwargs.pop("withdirs", True)
        entries, _ = await _collect_with_monitors(
            _tree_glob(
                self,
                _strip_only_protocol(self, path),
                maxdepth=maxdepth,
                withdirs=withdirs,
                ils_kwargs=kwargs,
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
            _tree_rm(self, _strip_only_protocol(self, path), maxdepth=maxdepth),
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
            _tree_cp(
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
        """Move files, as fsspec's ``mv`` does: with the tree move when recursive.

        A single file goes by the tree move too, recursive or not, so that it
        moves by ``_mv_file_entry``, the storage's rename where it has one.
        """
        if path1 == path2:
            return

        if not recursive and not await self._single_file(path1):
            # fsspec's own: a copy, then the deletion of its sources.
            await self._copy(path1, path2, recursive, "raise", maxdepth, **kwargs)
            await self._rm(path1, recursive=recursive)
            return

        _, errors = await _collect_with_monitors(
            _tree_mv(
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

    # fsspec makes sync methods only from the async ones it knows: _mv and _rmdir are
    # not. Each awaits the instance's own, so that a subclass's override is the one run.

    def mv(self, path1: str | list[str], path2: str, **kwargs: Any) -> None:
        return sync(self.loop, self._mv, path1, path2, **kwargs)

    def rmdir(self, path: str) -> None:
        return sync(self.loop, self._rmdir, path)
