# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A filesystem routing each path to the filesystem of its storage.

`AsyncRoutingFileSystem` is the base of filesystems spanning several storages
under one namespace, e.g. a cloud's volumes or containers, each with its own
filesystem. Each inner filesystem runs its own strategies. A copy or move
across two storages runs the source's family's strategy, as its profile's
``fs_type`` says, each file by ``_cross_fs_cp_entry`` or ``_cross_fs_mv_entry``,
which stream the data through the two storages' files unless overridden.

This module depends on `filesystem`, `operation` and `paths` of `common`. It
imports `cp` and `mv` of a family only to copy or move from a storage of it,
so a copy may leave out the other family.
"""

from __future__ import annotations

import asyncio
import os
from abc import abstractmethod
from contextlib import aclosing
from glob import has_magic
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from fsspec.asyn import AsyncFileSystem, sync
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractFileSystem

from fsspec_alt_strategies.monitor import Call, Monitored, routing

from .common.filesystem import (
    _AbstractFileSystemMeta,
    _copy_errors,
    _raise,
    _range_bounds,
    _strip_only_protocol,
)
from .common.operation import _collect_with_monitors
from .common.paths import _trim

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        Awaitable,
        Callable,
        Hashable,
        Sequence,
    )

    from fsspec.callbacks import Callback

    from .common.jobs import _Transfer
    from .common.operation import Operation

__all__ = ["AsyncRoutingFileSystem"]

_MiB = 1024 * 1024

_Group = tuple[AsyncFileSystem, list[str], list[str]]
"""An inner filesystem, the inner paths given to it, and the router's paths of them."""

_Backend = TypeVar("_Backend", bound=AsyncFileSystem)
"""The class of a router's inner filesystems, e.g. one per account of a cloud."""


class AsyncRoutingFileSystem(
    AsyncFileSystem, Monitored, Generic[_Backend], metaclass=_AbstractFileSystemMeta
):
    """An abstract async filesystem passing each call to the filesystem of the path's storage.

    It suits a namespace spanning several storages, each with a filesystem of
    its own, e.g. a cloud's volumes, or an account's containers each with its
    own client. A subclass says where each path goes, and the router passes
    each fsspec call on:

    - ``_route(path)`` gives the inner filesystem and its path, or None for a
      path above every storage, e.g. a catalog holding volumes.
    - ``_outer(fs, path)`` gives the router's path of an inner one, so that
      the names answered, e.g. by ``ls`` and ``find``, are the router's.
    - ``_roots(path)``, if any path is above the storages, lists a directory
      there, as ``ls`` with ``detail=True`` does.

    ``_route`` is sync, as sync methods route too. A subclass whose inner
    filesystems are all of one class names it, e.g.
    ``AsyncRoutingFileSystem[S3FileSystem]``, so that its hooks take that
    class.

    The primitives, e.g. ``_info``, ``_cat_file`` and ``_open``, call the
    inner filesystem's own. The recursive methods, e.g. ``_find``, ``_rm`` and
    ``_copy``, call the inner filesystem's own too, so that its strategies
    run. Several paths, e.g. of one ``rm``, are grouped by storage, each group
    sent at once. A failure raised by an inner filesystem gets a note naming
    the router's path, and its inner one.

    Above the storages, ``ls`` answers from ``_roots``, and ``info`` from
    ``_root_info``, which a subclass may override. ``find``, ``du`` and
    ``walk`` are fsspec's own walk, a directory after another, handing each
    storage's directory to its filesystem. A glob whose literal prefix
    routes goes whole to that filesystem. Another lists the prefix's roots, as
    fsspec's ``glob`` does. A change above the storages, e.g. ``rm`` of a
    catalog, raises PermissionError.

    A copy or move between two paths of one storage is the inner filesystem's
    own: both paths route to the same inner filesystem. Between two storages,
    it is the copy or move of the source's family, as its profile's ``fs_type``
    says: that of `TreeFileSystem` or `FlatFileSystem`, walking or listing the
    source as they do, with ``maxdepth`` too. Each file goes by
    ``_cross_fs_cp_entry`` or ``_cross_fs_mv_entry``, which stream the data
    unless a subclass overrides them, e.g. with a server-side copy. A source of
    unknown ``fs_type`` is copied by fsspec's own copy, a file at a time. A
    move between two storages deletes each source file once its copy succeeds,
    and the source directories left empty.

    Inner filesystems are async, on the router's loop: a sync one raises
    TypeError, and is to be wrapped, e.g. in a `SyncTreeWrapper`. A subclass
    makes them when first needed, with `_backend`, so that ``close()`` closes
    them, and `backends_loop` tells the loop they run on. Pickling keeps the
    constructor's arguments, as fsspec's own does, so a copy makes its inner
    filesystems again.

    One transaction is the router's: ``open`` passes ``autocommit`` on, and
    fsspec adds each file written to the router's transaction. Its commit, or
    discard, is file by file: it is not atomic across storages. In it,
    ``pipe_file`` and ``put_file`` write through ``open``, so that their files
    join it. ``open_async`` takes no part in it, as in fsspec, whose
    ``open_async`` knows no transaction: its files are the inner filesystem's
    own, written as they close. Neither does a copy between two storages,
    which streams through ``open_async``.

    The router has no ``storage_profile``: each storage has its own, which
    its inner filesystem gives. Its only limit is ``transfer_limit``, the
    files copied across storages at once. The inner filesystems keep their
    own limits.

    A `Monitor` sees the calls of the inner filesystems, their ``Call.via``
    naming the routers they came through. A copy across storages is a call of
    the router itself.

    Parameters
    ----------
    transfer_concurrency : int
        The files copied between two storages at a time.
    **kwargs
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If ``transfer_concurrency`` is less than 1.

    See Also
    --------
    TreeFileSystem, FlatFileSystem : The inner filesystems it suits best.

    Examples
    --------
    >>> class Lake(AsyncRoutingFileSystem):
    ...     protocol = "lake"
    ...
    ...     def _route(self, path):
    ...         volume, _, inner = path.partition("/")
    ...         if not volume:
    ...             return None  # The root, above every volume.
    ...         return self._backend(volume, lambda: self._volume(volume)), inner
    ...
    ...     def _outer(self, fs, path):
    ...         return f"{fs.volume}/{path}".rstrip("/")
    ...
    ...     async def _roots(self, path):
    ...         return [
    ...             {"name": v, "type": "directory", "size": 0}
    ...             for v in await self.client.list_volumes()
    ...         ]
    >>> fs = Lake(transfer_concurrency=8)
    """

    def __init__(self, *args: Any, transfer_concurrency: int, **kwargs: Any) -> None:
        if transfer_concurrency < 1:
            raise ValueError(
                f"transfer_concurrency must be at least 1: {transfer_concurrency}"
            )

        super().__init__(*args, **kwargs)

        self.transfer_concurrency = transfer_concurrency
        self.transfer_limit = asyncio.Semaphore(transfer_concurrency)
        self._backends: dict[Hashable, _Backend] = {}
        self._backends_loop: asyncio.AbstractEventLoop | None = None

    # What a subclass implements.

    @abstractmethod
    def _route(self, path: str) -> tuple[_Backend, str] | None:
        """The inner filesystem of a path, and its path there, or None above them all.

        ``path`` is the router's, without the protocol. A trailing "/" is kept,
        as it means a directory to a copy or a glob: the router puts it back on
        the inner path if ``_route`` drops it. ``path`` may be a glob pattern
        too: where its literal part routes, the whole pattern must route to the
        same filesystem.

        It raises FileNotFoundError for a path no storage has, e.g. a missing
        volume. It may make the inner filesystem, with `_backend`.
        """

    @abstractmethod
    def _outer(self, fs: _Backend, path: str) -> str:
        """The router's path of ``path``, a path of the inner filesystem ``fs``."""

    # What a subclass may override.

    async def _roots(self, path: str) -> list[dict[str, Any]]:
        """List a directory above the storages, as ``_ls`` with ``detail=True`` does.

        Each entry's name is the router's path. It raises FileNotFoundError
        for a directory that does not exist.

        Here, there is none: ``_route`` never answers None, e.g. for URLs each
        naming its storage, and any path raises FileNotFoundError. A subclass
        with paths above its storages overrides it.
        """
        raise FileNotFoundError(path)

    async def _root_info(self, path: str) -> dict[str, Any]:
        """Describe a path above the storages, as ``_info`` does.

        ``path`` is the router's, without the protocol. Here, the root is a
        bare directory entry, and any other path the entry of its name in
        ``_roots`` of its parent. A subclass knowing more of them, e.g. the
        root's own entry, or one request describing a path, overrides it.
        It raises FileNotFoundError for a path that does not exist.
        """
        if path in ("", self.root_marker):
            return {"name": path, "size": 0, "type": "directory"}

        for entry in await self._roots(self._parent(path)):
            if entry["name"].rstrip("/") == path:
                return entry

        raise FileNotFoundError(path)

    async def _cross_fs_cp_entry(
        self,
        src_fs: _Backend,
        src_entry: dict[str, Any],
        dest_fs: _Backend,
        dest_path: str,
    ) -> None:
        """Copy a file from one storage to another, the paths the inner filesystems'.

        ``src_entry`` is the source's, as its listing or ``_info`` gave it, its
        ``"name"`` the source path. Its size, e.g., may choose how the data
        goes. It may be stale, as for `TreeFileSystem._cp_file_entry`.

        Here, the data is streamed through the router, by both storages'
        ``open_async``. A block, the source's ``transfer_size``, is read, then
        written, before the next is read: a copy holds one block in memory,
        however large the file. A copy failing midway leaves what the
        destination's file leaves on an exception, e.g. an adaptive file aborts
        its upload.

        A subclass overrides it with the cloud's server-side copy, e.g. a copy
        from a URL, and falls back on this one where that cannot be done, e.g.
        between two clouds. It runs holding a permit of ``transfer_limit``.
        """
        block_size = getattr(src_fs, "transfer_size", None) or 8 * _MiB

        reader = await src_fs.open_async(src_entry["name"], "rb")
        async with reader:
            writer = await dest_fs.open_async(dest_path, "wb")
            async with writer:
                while block := await reader.read(block_size):
                    await writer.write(block)

    async def _cross_fs_mv_entry(
        self,
        src_fs: _Backend,
        src_entry: dict[str, Any],
        dest_fs: _Backend,
        dest_path: str,
    ) -> None:
        """Move a file from one storage to another, the paths the inner filesystems'.

        Here, it is ``_cross_fs_cp_entry``, then the source's ``_rm_file``: a
        source whose copy failed is kept. It runs holding a permit of
        ``transfer_limit``.
        """
        await self._cross_fs_cp_entry(src_fs, src_entry, dest_fs, dest_path)
        await src_fs._rm_file(src_entry["name"])

    def _backend(self, key: Hashable, make: Callable[[], _Backend]) -> _Backend:
        """The inner filesystem of ``key``, made by ``make`` the first time.

        A subclass's ``_route`` makes its inner filesystems with it, e.g. one
        per volume, or per account and container. ``make`` gives them the
        router's mode and loop, ``asynchronous=self.asynchronous`` and
        ``loop=self.loop``: a sync router's calls, e.g. ``open`` and ``sign``,
        call their sync methods, which run on that loop. Made async, they would
        have none. ``close()`` closes them.
        """
        fs = self._backends.get(key)
        if fs is None:
            fs = self._backends[key] = make()
            if self._backends_loop is None:
                try:
                    self._backends_loop = asyncio.get_running_loop()
                except RuntimeError:
                    self._backends_loop = self._loop
        return fs

    @property
    def backends_loop(self) -> asyncio.AbstractEventLoop | None:
        """The loop the inner filesystems run on, once one is made, else None.

        It is the loop running when the first was made, or else a sync router's
        own. Their clients' sessions are bound to it. A subclass holding what
        they share, e.g. a credential, closes it there, after ``close()``.
        """
        return self._backends_loop

    def close(self) -> None:
        """Close the inner filesystems made by `_backend`, and forget them, and their loop.

        Each is closed by its own ``close()``, if it has one. A call after
        makes them again. A subclass closing what they share takes
        `backends_loop` first.
        """
        backends, self._backends = self._backends, {}
        self._backends_loop = None
        for fs in backends.values():
            close = getattr(fs, "close", None)
            if callable(close):
                close()

    # Routing.

    def _target(self, path: str) -> tuple[AsyncFileSystem, str] | None:
        """``_route`` of a path given to a method, refusing a sync filesystem.

        A trailing "/" of the path is kept on a non-empty inner path, as it
        means a directory to a copy or a glob: ``_route`` need not keep it.
        """
        plain = _strip_only_protocol(self, path)
        target = self._route(plain)
        if target is None:
            return None

        fs, inner = target
        if not getattr(fs, "async_impl", False):
            raise TypeError(
                f"{fs!r} is not async: a router passes its calls on to async "
                "filesystems only. Wrap it, e.g. in a SyncTreeWrapper."
            )

        if plain.endswith("/") and inner and not inner.endswith("/"):
            return fs, f"{inner}/"
        else:
            return target

    def _required(self, path: str) -> tuple[AsyncFileSystem, str]:
        """``_target`` of a path that must be in a storage, e.g. of a file."""
        target = self._target(path)
        if target is None:
            raise PermissionError(
                f"{path!r} is above the storages of {self!r}: it can only be listed."
            )

        return target

    def _pattern_target(self, path: str) -> tuple[AsyncFileSystem, str] | None:
        """``_target`` of a path or glob pattern, if all it matches is in one storage."""
        if not has_magic(path):
            return self._target(path)

        literal = path[: min(path.find(c) % (len(path) + 1) for c in "*?[")]
        base = self._target(literal.rpartition("/")[0])
        if base is None:
            return None

        whole = self._target(path)
        return whole if whole is not None and whole[0] is base[0] else None

    async def _grouped(self, path: str | Sequence[str]) -> list[_Group]:
        """The paths of a call changing the storages, grouped by inner filesystem.

        A pattern matching in several storages is expanded by ``_glob`` first.
        A path above the storages raises PermissionError.
        """
        groups: dict[int, _Group] = {}

        def add(target: tuple[AsyncFileSystem, str], outer: str) -> None:
            fs, inner = target
            group = groups.setdefault(id(fs), (fs, [], []))
            group[1].append(inner)
            group[2].append(outer)

        for p in [path] if isinstance(path, str) else path:
            target = self._pattern_target(_strip_only_protocol(self, p))
            if target is not None:
                add(target, p)
            elif has_magic(p):
                for match in await self._glob(p):
                    add(self._required(match), match)
            else:
                self._required(p)  # Raises, above the storages.

        return list(groups.values())

    def _note(
        self, error: BaseException, fs: AsyncFileSystem, inner: Any, outer: Any
    ) -> None:
        """Tell the router's path in an inner filesystem's error."""
        if isinstance(error, OSError) and error.filename == inner:
            error.filename = outer
        error.add_note(f"{outer!r} was routed to {fs!r} as {inner!r}.")

    async def _on(
        self, fs: AsyncFileSystem, inner: Any, outer: Any, call: Awaitable[Any]
    ) -> Any:
        """Await an inner filesystem's call, as made through this router."""
        with routing(self):
            try:
                return await call
            except Exception as e:
                self._note(e, fs, inner, outer)
                raise

    async def _forward(self, path: str, method: str, *args: Any, **kwargs: Any) -> Any:
        """Call the inner filesystem's ``method`` on the inner path of ``path``."""
        fs, inner = self._required(path)
        return await self._on(
            fs, inner, path, getattr(fs, method)(inner, *args, **kwargs)
        )

    def _entry(self, fs: AsyncFileSystem, entry: dict[str, Any]) -> dict[str, Any]:
        """An inner filesystem's entry, named by the router's path."""
        return {**entry, "name": self._outer(fs, entry["name"])}

    async def _each(
        self,
        groups: list[_Group],
        run: Callable[[_Group], Awaitable[Any]],
        doing: str,
        errors: Sequence[Exception] = (),
    ) -> None:
        """Run a call per group at once, raising their errors when all are done.

        No group is stopped by another's failure: a deletion or copy under
        way finishes, as its outcome would otherwise be unknown. ``errors``
        are those found before, raised with the groups'.
        """
        outcomes = await asyncio.gather(*map(run, groups), return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException) and not isinstance(
                outcome, Exception
            ):
                raise outcome  # A cancellation, never swallowed.

        _raise([*errors, *(o for o in outcomes if isinstance(o, Exception))], doing)

    def _placed(
        self, paths: Sequence[str], destination: str, doing: str
    ) -> tuple[list[str], list[Exception]]:
        """The roots of a list copied or moved into ``destination``, each name once.

        Each root goes to the destination under its name, whatever its
        storage, as a family's copy of a list places it. Two roots of one name
        would go to the same place: as there, the first in path order goes,
        and each other is a FileExistsError. Roots in different storages are
        checked here, as no inner filesystem sees them both. A pattern is
        left to the filesystem expanding it.
        """
        placed: dict[str, str] = {}
        errors: list[Exception] = []
        skipped: set[str] = set()
        base = _trim(_strip_only_protocol(self, destination))

        for path in sorted(paths, key=lambda p: _trim(_strip_only_protocol(self, p))):
            if has_magic(path):
                continue

            name = _trim(_strip_only_protocol(self, path)).rpartition("/")[2]
            target = f"{base}/{name}" if base else name
            if (other := placed.get(target)) is not None:
                error = FileExistsError(target)
                error.add_note(
                    f"{doing} {path!r} to {target!r} skipped: {other!r} goes there too."
                )
                errors.append(error)
                skipped.add(path)
                continue

            placed[target] = path

        return [p for p in paths if p not in skipped], errors

    @staticmethod
    def _one(paths: list[str]) -> str | list[str]:
        """A group's paths as given to a method: one alone, as fsspec's semantics differ."""
        return paths[0] if len(paths) == 1 else paths

    # The primitives, each its storage's own.

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        target = self._target(path)
        if target is not None:
            fs, inner = target
            return self._entry(fs, await self._on(fs, inner, path, fs._info(inner)))
        else:
            return await self._root_info(self._strip_protocol(path))

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        target = self._target(path)
        if target is None:
            entries = await self._roots(self._strip_protocol(path))
        else:
            fs, inner = target
            listed = await self._on(
                fs, inner, path, fs._ls(inner, detail=True, **kwargs)
            )
            entries = [self._entry(fs, e) for e in listed]

        return entries if detail else [e["name"] for e in entries]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        return await self._forward(path, "_cat_file", start=start, end=end, **kwargs)

    async def _cat_ranges(
        self,
        paths: list[str],
        starts: Any,
        ends: Any,
        max_gap: int | None = None,
        batch_size: int | None = None,
        on_error: str = "return",
        **kwargs: Any,
    ) -> list[Any]:
        """Read byte ranges of files, as fsspec's ``cat_ranges`` does, each storage's by its own.

        fsspec's calls ``_cat_file`` once per range. This gives each inner
        filesystem its ranges in one ``_cat_ranges``, all at once, so that a
        filesystem merging nearby ranges, as the families' do, merges them. A
        path no storage has, or above them, fails in place of its range. The
        answers come in the order asked, as fsspec's do.
        """
        starts, ends = _range_bounds(paths, starts, ends)
        out: list[Any] = [None] * len(paths)
        # Each inner filesystem with the ranges it reads: their indices, and inner paths.
        groups: dict[int, tuple[AsyncFileSystem, list[int], list[str]]] = {}
        for i, path in enumerate(paths):
            try:
                fs, inner = self._required(path)
            except Exception as e:
                out[i] = e
                continue
            groups.setdefault(id(fs), (fs, [], []))[1].append(i)
            groups[id(fs)][2].append(inner)

        async def read(
            fs: AsyncFileSystem, indices: list[int], inners: list[str]
        ) -> None:
            options = {} if max_gap is None else {"max_gap": max_gap}
            with routing(self):
                results = await fs._cat_ranges(
                    inners,
                    [starts[i] for i in indices],
                    [ends[i] for i in indices],
                    batch_size=batch_size,
                    on_error="return",
                    **options,
                    **kwargs,
                )
            for i, inner, result in zip(indices, inners, results, strict=True):
                if isinstance(result, BaseException):
                    self._note(result, fs, inner, paths[i])
                out[i] = result

        await asyncio.gather(*(read(*group) for group in groups.values()))
        if on_error != "return":
            for result in out:
                if isinstance(result, BaseException):
                    raise result
        return out

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        if self._intrans:
            pipe_file = AbstractFileSystem.pipe_file
            await self._in_transaction(path, pipe_file, path, value, **kwargs)
            return

        await self._forward(path, "_pipe_file", value, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        await self._forward(rpath, "_get_file", lpath, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        if self._intrans:
            await self._in_transaction(rpath, _put_file, lpath, rpath, **kwargs)
            return

        fs, inner = self._required(rpath)
        await self._on(fs, inner, rpath, fs._put_file(lpath, inner, **kwargs))

    async def _in_transaction(
        self, path: str, write: Callable[..., None], *args: Any, **kwargs: Any
    ) -> None:
        """Write ``path`` by fsspec's sync ``write``, through ``open``, so that it joins the transaction.

        An inner filesystem's own write goes through ``open_async``, whose
        files know no transaction. ``write`` takes the router first, as
        fsspec's sync methods do. The sync file runs in a thread, as its
        methods may wait on this loop.
        """
        fs, inner = self._required(path)
        await self._on(fs, inner, path, asyncio.to_thread(write, self, *args, **kwargs))

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        await self._forward(path, "_rm_file", **kwargs)

    async def _rmdir(self, path: str) -> None:
        await self._forward(path, "_rmdir")

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        await self._forward(path, "_mkdir", create_parents=create_parents, **kwargs)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        if self._target(path) is None and await self._exists(path):
            if exist_ok:
                return
            raise FileExistsError(path)

        await self._forward(path, "_makedirs", exist_ok=exist_ok)

    async def _exists(self, path: str, **kwargs: Any) -> bool:
        try:
            target = self._target(path)
        except FileNotFoundError:
            return False

        if target is None:
            return await super()._exists(path, **kwargs)

        fs, inner = target
        return await self._on(fs, inner, path, fs._exists(inner, **kwargs))

    async def _isdir(self, path: str) -> bool:
        try:
            target = self._target(path)
        except FileNotFoundError:
            return False

        if target is None:
            return await super()._isdir(path)

        fs, inner = target
        return await self._on(fs, inner, path, fs._isdir(inner))

    async def _isfile(self, path: str) -> bool:
        try:
            target = self._target(path)
        except FileNotFoundError:
            return False

        if target is None:
            return False  # Above the storages, all are directories.

        fs, inner = target
        return await self._on(fs, inner, path, fs._isfile(inner))

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        fs1, inner1 = self._required(path1)
        fs2, inner2 = self._required(path2)

        if fs1 is fs2:
            await self._on(fs1, inner1, path1, fs1._cp_file(inner1, inner2, **kwargs))
            return

        # The source's entry, for the copy across, tells a directory too.
        entry = await self._on(fs1, inner1, path1, fs1._info(inner1))
        if entry["type"] == "directory":
            # fsspec's copy gives directories too, which each filesystem makes.
            await self._on(fs2, inner2, path2, fs2._makedirs(inner2, exist_ok=True))
        else:
            copy_file = self._across(fs1, fs2)
            entry = {**entry, "name": inner1}
            await self._on(fs1, inner1, path1, copy_file(entry, inner2))

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        fs1, inner1 = self._required(path1)
        fs2, inner2 = self._required(path2)

        if fs1 is fs2:
            await self._on(fs1, inner1, path1, fs1._mv_file(inner1, inner2, **kwargs))
        else:
            entry = await self._on(fs1, inner1, path1, fs1._info(inner1))
            mv_file = self._across(fs1, fs2, move=True)
            entry = {**entry, "name": inner1}
            await self._on(fs1, inner1, path1, mv_file(entry, inner2))

    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return await self._forward(path, "open_async", mode, **kwargs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        # autocommit is passed on: fsspec's open adds the file to the router's
        # transaction, which commits it with the others.
        fs, inner = self._required(path)
        return fs._open(
            inner,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def created(self, path: str) -> Any:
        fs, inner = self._required(path)
        return fs.created(inner)

    def modified(self, path: str) -> Any:
        fs, inner = self._required(path)
        return fs.modified(inner)

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> str:
        fs, inner = self._required(path)
        return fs.sign(inner, expiration=expiration, **kwargs)

    def invalidate_cache(self, path: str | None = None) -> None:
        super().invalidate_cache(path)

        if path is None:
            for fs in self._backends.values():
                fs.invalidate_cache()
            return

        target = self._target(path)
        if target is not None:
            target[0].invalidate_cache(target[1])

    # The recursive methods, each its storage's own, so that its strategies run.

    async def _walk(
        self, path: str, maxdepth: int | None = None, **kwargs: Any
    ) -> AsyncGenerator[tuple[str, Any, Any]]:
        target = self._target(path)
        if target is None:
            # fsspec's walk, listing from _roots, then each storage's own below.
            async for item in super()._walk(path, maxdepth=maxdepth, **kwargs):
                yield item
            return

        fs, inner = target
        detail = kwargs.get("detail", False)
        walk = fs._walk(inner, maxdepth=maxdepth, **kwargs)

        async with aclosing(walk):
            while True:
                # A step at a time, as the generator runs in its caller's context.
                with routing(self):
                    try:
                        root, dirs, files = await anext(walk)
                    except StopAsyncIteration:
                        return
                    except Exception as e:
                        self._note(e, fs, inner, path)
                        raise

                if detail:
                    dirs = {k: self._entry(fs, v) for k, v in dirs.items()}
                    files = {k: self._entry(fs, v) for k, v in files.items()}

                yield self._outer(fs, root), dirs, files

    async def _find(
        self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> Any:
        target = self._target(path)
        detail = kwargs.pop("detail", False)
        if target is None:
            return await super()._find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
            )

        fs, inner = target
        found = await self._on(
            fs,
            inner,
            path,
            fs._find(
                inner, maxdepth=maxdepth, withdirs=withdirs, detail=True, **kwargs
            ),
        )

        return self._found(fs, found, detail)

    def _found(self, fs: AsyncFileSystem, found: dict[str, Any], detail: bool) -> Any:
        """An inner ``find`` or ``glob``'s answer with details, as the router's."""
        entries = {self._outer(fs, n): self._entry(fs, e) for n, e in found.items()}
        names = sorted(entries)
        return {n: entries[n] for n in names} if detail else names

    async def _du(
        self, path: str, total: bool = True, maxdepth: int | None = None, **kwargs: Any
    ) -> Any:
        target = self._target(path)
        if target is None:
            return await super()._du(path, total=total, maxdepth=maxdepth, **kwargs)

        fs, inner = target
        sizes = await self._on(
            fs, inner, path, fs._du(inner, total=total, maxdepth=maxdepth, **kwargs)
        )

        return sizes if total else {self._outer(fs, n): s for n, s in sizes.items()}

    async def _glob(self, path: str, maxdepth: int | None = None, **kwargs: Any) -> Any:
        target = self._pattern_target(_strip_only_protocol(self, path))
        detail = kwargs.pop("detail", False)
        if target is None:
            # fsspec's glob, finding from the literal prefix over _roots.
            return await super()._glob(path, maxdepth=maxdepth, detail=detail, **kwargs)

        fs, inner = target
        found = await self._on(
            fs, inner, path, fs._glob(inner, maxdepth=maxdepth, detail=True, **kwargs)
        )

        return self._found(fs, found, detail)

    async def _expand_path(
        self,
        path: str | list[str],
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> list[str]:
        target = (
            self._pattern_target(_strip_only_protocol(self, path))
            if isinstance(path, str)
            else None
        )

        if target is None:
            return await super()._expand_path(
                path, recursive=recursive, maxdepth=maxdepth, **kwargs
            )

        fs, inner = target
        expanded = await self._on(
            fs,
            inner,
            path,
            fs._expand_path(inner, recursive=recursive, maxdepth=maxdepth, **kwargs),
        )

        return sorted(self._outer(fs, p) for p in expanded)

    async def _rm(
        self,
        path: str | list[str],
        recursive: bool = False,
        batch_size: int | None = None,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        if maxdepth is not None:
            kwargs["maxdepth"] = maxdepth

        async def rm(group: _Group) -> None:
            fs, inner, outer = group
            await self._on(
                fs,
                self._one(inner),
                self._one(outer),
                fs._rm(
                    self._one(inner),
                    recursive=recursive,
                    batch_size=batch_size,
                    **kwargs,
                ),
            )

        await self._each(await self._grouped(path), rm, "Deleting")

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
        """Copy, as fsspec's ``copy`` does: within a storage, or between two.

        Sources in several storages are copied per storage, at once. A list
        stays a list in each storage, so that each root goes into the
        destination under its name, as `_placed` checks across storages.
        """
        if not isinstance(path2, str):
            # Pairs of paths: fsspec's, a _cp_file per pair.
            await super()._copy(
                path1, path2, recursive, on_error, maxdepth, batch_size, **kwargs
            )
            return

        fs2, inner2 = self._required(path2)
        callback = kwargs.get("callback")
        listed = not isinstance(path1, str)
        clashes: list[Exception] = []
        if listed:
            path1, clashes = self._placed(path1, path2, "Copying")

        async def copy(group: _Group) -> None:
            fs1, inner1, outer1 = group
            sources = inner1 if listed else self._one(inner1)
            outers = outer1 if listed else self._one(outer1)

            if fs1 is fs2:
                await self._on(
                    fs1,
                    sources,
                    outers,
                    fs1._copy(
                        sources,
                        inner2,
                        recursive,
                        on_error,
                        maxdepth,
                        batch_size,
                        **kwargs,
                    ),
                )
            elif recursive and (
                op := await self._cross_fs_cp(fs1, sources, fs2, inner2, maxdepth)
            ):
                errors = await self._transferred(
                    op, fs1, sources, fs2, inner2, outers, path2, callback
                )
                _raise(_copy_errors(errors, on_error), "Copying")
            else:
                await super(AsyncRoutingFileSystem, self)._copy(
                    outers,
                    path2,
                    recursive,
                    on_error,
                    maxdepth,
                    batch_size,
                    **kwargs,
                )

        await self._each(await self._grouped(path1), copy, "Copying", clashes)

    async def _mv(
        self,
        path1: str | list[str],
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Move, as fsspec's ``mv`` does: within a storage, or between two.

        Between two storages, each source file is deleted once copied, and the
        source directories left empty once all are. A source whose copy failed
        is kept.
        """
        if path1 == path2:
            return

        fs2, inner2 = self._required(path2)
        listed = not isinstance(path1, str)
        clashes: list[Exception] = []
        if listed:
            path1, clashes = self._placed(path1, path2, "Moving")

        async def mv(group: _Group) -> None:
            fs1, inner1, outer1 = group
            sources = inner1 if listed else self._one(inner1)
            outer = outer1 if listed else self._one(outer1)

            if fs1 is fs2:
                own = getattr(fs1, "_mv", None)
                if own is not None:
                    await self._on(
                        fs1,
                        sources,
                        outer,
                        own(
                            sources,
                            inner2,
                            recursive=recursive,
                            maxdepth=maxdepth,
                            **kwargs,
                        ),
                    )
                    return

                # A plain fsspec filesystem has no async mv: its sync one's way.
                await self._on(
                    fs1,
                    sources,
                    outer,
                    fs1._copy(sources, inner2, recursive, "raise", maxdepth, **kwargs),
                )
                await self._on(
                    fs1, sources, outer, fs1._rm(sources, recursive=recursive)
                )
            elif recursive and (
                op := await self._cross_fs_mv(fs1, sources, fs2, inner2, maxdepth)
            ):
                errors = await self._transferred(
                    op,
                    fs1,
                    sources,
                    fs2,
                    inner2,
                    outer,
                    path2,
                    kwargs.get("callback"),
                    delete=True,
                )
                _raise(errors, "Moving")
            else:
                await super(AsyncRoutingFileSystem, self)._copy(
                    outer, path2, recursive, "raise", maxdepth, **kwargs
                )
                await self._rm(outer, recursive=recursive)

        await self._each(await self._grouped(path1), mv, "Moving", clashes)

    async def _cross_fs_cp(
        self,
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        maxdepth: int | None = None,
    ) -> Operation[tuple[str, str], Any] | None:
        """The recursive copy from ``fs1`` to ``fs2``, by the source's family, or None.

        The family is the source's profile's ``fs_type``: None when unknown, and
        fsspec's own copy then runs. Each file goes by ``_cross_fs_cp_entry``.
        The operation is returned not yet started.
        """
        get = getattr(fs1, "_get_storage_profile", None)
        if get is None:
            return None  # A plain fsspec filesystem: fsspec's own copy.
        fs_type = (await get(path1)).fs_type
        if fs_type is None:
            return None

        # A family's strategies are imported only when used, so that a copy may
        # leave out the other family.
        if fs_type == "tree":
            from .tree.cp import _tree_cp  # noqa: PLC0415

            return _tree_cp(
                fs1,
                fs2,
                path1,
                path2,
                cp_entry=self._across(fs1, fs2),
                same_storage=False,
                maxdepth=maxdepth,
            )
        if fs_type == "flat":
            from .flat.cp import _flat_cp  # noqa: PLC0415

            return _flat_cp(
                fs1,
                fs2,
                path1,
                path2,
                cp_entry=self._across(fs1, fs2),
                same_storage=False,
                maxdepth=maxdepth,
            )
        else:
            return None

    async def _cross_fs_mv(
        self,
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        maxdepth: int | None = None,
    ) -> Operation[tuple[str, str], Any] | None:
        """The recursive move from ``fs1`` to ``fs2``, by the source's family, or None.

        As `_cross_fs_cp`, each file by ``_cross_fs_mv_entry``. A flat source
        copies each key by ``_cross_fs_cp_entry`` instead, and deletes the keys
        copied in batches, by its ``_rm_files``.
        """
        get = getattr(fs1, "_get_storage_profile", None)
        if get is None:
            return None  # A plain fsspec filesystem: fsspec's own copy, then rm.
        fs_type = (await get(path1)).fs_type
        if fs_type is None:
            return None

        # A family's strategies are imported only when used, so that a copy may
        # leave out the other family.
        if fs_type == "tree":
            from .tree.mv import _tree_mv  # noqa: PLC0415

            return _tree_mv(
                fs1,
                fs2,
                path1,
                path2,
                mv_entry=self._across(fs1, fs2, move=True),
                same_storage=False,
                maxdepth=maxdepth,
            )
        if fs_type == "flat":
            from .flat.mv import _flat_mv  # noqa: PLC0415

            # By copies, the keys copied deleted in batches by the source's
            # _rm_files: two storages never rename.
            return _flat_mv(
                fs1,
                fs2,
                path1,
                path2,
                cp_entry=self._across(fs1, fs2),
                same_storage=False,
                maxdepth=maxdepth,
            )
        else:
            return None

    def _across(
        self, fs1: AsyncFileSystem, fs2: AsyncFileSystem, *, move: bool = False
    ) -> _Transfer:
        """A file's copy, or move, from ``fs1`` to ``fs2``, under ``transfer_limit``."""
        hook = self._cross_fs_mv_entry if move else self._cross_fs_cp_entry

        async def request(entry: dict[str, Any], path2: str) -> None:
            with routing(self):
                async with self.transfer_limit:
                    await hook(fs1, entry, fs2, path2)

        return request

    async def _transferred(
        self,
        op: Operation[tuple[str, str], Any],
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        outer1: str | list[str],
        outer2: str,
        callback: Any,
        delete: bool = False,
    ) -> list[Exception]:
        """Run a copy or move between two storages, returning its paths' errors.

        The call is the router's own, watched by the monitors as such.
        """
        call = Call(self, "mv" if delete else "copy", outer1, outer2)
        _, errors = await _collect_with_monitors(op, call, callback)
        for error in errors:
            error.add_note(
                f"Through {self!r}, from {fs1!r} as {path1!r} to {fs2!r} as {path2!r}."
            )

        return errors

    # fsspec makes sync methods only from the async ones it knows: _mv and
    # _rmdir are not.

    def mv(self, path1: str | list[str], path2: str, **kwargs: Any) -> None:
        return sync(self.loop, self._mv, path1, path2, **kwargs)

    def rmdir(self, path: str) -> None:
        return sync(self.loop, self._rmdir, path)


def _put_file(
    fs: AbstractFileSystem,
    lpath: str,
    rpath: str,
    mode: str = "overwrite",
    callback: Callback = DEFAULT_CALLBACK,
    **kwargs: Any,
) -> None:
    """fsspec's ``put_file`` through ``open``, without its ``makedirs``.

    A storage needs no directory for the files put in it, and fsspec's sync
    ``makedirs`` cannot run from a thread of an async router.
    """
    if os.path.isdir(lpath):
        return

    with open(lpath, "rb") as local:
        callback.set_size(os.fstat(local.fileno()).st_size)
        with fs.open(rpath, "xb" if mode == "create" else "wb", **kwargs) as f:
            while chunk := local.read(fs.blocksize):
                f.write(chunk)
                callback.relative_update(len(chunk))
