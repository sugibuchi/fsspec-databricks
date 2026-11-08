# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A filesystem routing each path to the filesystem of its storage.

`AsyncRoutingFileSystem` is the base of filesystems spanning several storages
under one namespace, e.g. a cloud's volumes or containers, each with its own
filesystem. It knows no family: each inner filesystem runs its own strategies,
and a copy across two storages runs the strategy its subclass's ``_transfer``
picks. `streaming_copy`, the copy of a file it falls back on, streams the
data through the two storages' files.

This module depends on `filesystem`, `operation`, `paths` and `types`.
"""

from __future__ import annotations

import asyncio
import os
from abc import abstractmethod
from contextlib import aclosing, suppress
from glob import has_magic
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem, sync
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractFileSystem

from fsspec_alt_strategies_common.monitor import Call, Monitored, routing
from fsspec_alt_strategies_common.profile import StorageProfile

from .filesystem import (
    _AbstractFileSystemMeta,
    _copy_errors,
    _raise,
    _strip_only_protocol,
)
from .operation import _collect_with_monitors
from .paths import _trim

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        Awaitable,
        Callable,
        Hashable,
        Sequence,
    )

    from fsspec.callbacks import Callback

    from fsspec_alt_strategies_common.states import CpState

    from .operation import Operation
    from .types import _Request

__all__ = ["AsyncRoutingFileSystem", "streaming_copy"]

_MiB = 1024 * 1024

_Group = tuple[AsyncFileSystem, list[str], list[str]]
"""An inner filesystem, the inner paths given to it, and the router's paths of them."""


def streaming_copy(
    open_source: Callable[..., Awaitable[Any]],
    open_destination: Callable[..., Awaitable[Any]],
    *,
    block_size: int = 8 * _MiB,
) -> _Request:
    """Make a ``copy_file(source, destination)`` streaming the data between two storages.

    Each storage's ``open_async`` serves, called as ``open(path, "rb")`` and
    ``open(path, "wb")``. The file objects are awaited, as those of
    ``open_async`` are. A block is read, then written, before the next is
    read. So a copy holds one block in memory, however large the file.

    A copy failing midway leaves what the destination's file object leaves
    on an exception in its ``async with`` block. An adaptive file aborts its
    upload, for example, while a plain one may keep the blocks written.

    It suits any two storages, but every byte goes through the caller. A
    server-side copy between two storages of one cloud wins where it works:
    a caller's ``copy_file`` may try it first, and fall back on this one, as
    `AsyncRoutingFileSystem` does.

    Parameters
    ----------
    open_source : callable
        Opens a source file, e.g. the source filesystem's ``open_async``.
    open_destination : callable
        Opens a destination file, e.g. the destination's ``open_async``.
    block_size : int
        The bytes read, then written, at a time.

    Returns
    -------
    callable
        The ``copy_file``, a coroutine function.

    Raises
    ------
    ValueError
        If ``block_size`` is less than 1.

    See Also
    --------
    tree_transfer, flat_transfer : The copies taking it.

    Examples
    --------
    >>> copy_file = streaming_copy(src.open_async, dst.open_async)
    >>> await copy_file("data/a.parquet", "backup/a.parquet")
    """
    if block_size < 1:
        raise ValueError(f"block_size must be at least 1: {block_size}")

    async def copy_file(source: str, destination: str) -> None:
        reader = await open_source(source, "rb")
        async with reader:
            writer = await open_destination(destination, "wb")
            async with writer:
                while block := await reader.read(block_size):
                    await writer.write(block)

    return copy_file


class AsyncRoutingFileSystem(
    AsyncFileSystem, Monitored, metaclass=_AbstractFileSystemMeta
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
    - ``_roots(path)`` lists a folder above the storages, as ``ls`` with
      ``detail=True`` does.

    ``_route`` is sync, as sync methods route too.

    The primitives, e.g. ``_info``, ``_cat_file`` and ``_open``, call the
    inner filesystem's own. The recursive methods, e.g. ``_find``, ``_rm`` and
    ``_copy``, call the inner filesystem's own too, so that its strategies
    run. Several paths, e.g. of one ``rm``, are grouped by storage, each group
    sent at once. A failure raised by an inner filesystem gets a note naming
    the router's path, and its inner one.

    Above the storages, ``ls`` answers from ``_roots``, and ``info`` from
    ``_root_info``, which a subclass may override. ``find``,
    ``du`` and ``walk`` are fsspec's own walk, a folder after another, handing
    each storage's folder to its filesystem. A glob whose literal prefix
    routes goes whole to that filesystem. Another lists the prefix's roots, as
    fsspec's ``glob`` does. A change above the storages, e.g. ``rm`` of a
    catalog, raises PermissionError.

    A copy or move between two paths of one storage is the inner filesystem's
    own: both paths route to the same inner filesystem. Between two storages, it is
    the strategy ``_transfer`` returns, e.g. `tree_transfer`, given a
    ``copy_file`` trying ``_server_copy``, then streaming the data. Where it
    returns None, as it does unless overridden, it is fsspec's own copy, a
    file at a time. A move between two storages deletes each source file once its
    copy succeeds, then the source folders left empty.

    Inner filesystems are async, on the router's loop: a sync one raises
    TypeError, and is to be wrapped, e.g. in a `SyncTreeWrapper`. A subclass
    makes them when first needed, with `_backend`, so that ``close()`` closes
    them. Pickling keeps the constructor's arguments, as fsspec's own does, so
    a copy makes its inner filesystems again.

    One transaction is the router's: ``open`` passes ``autocommit`` on, and
    fsspec adds each file written to the router's transaction. Its commit, or
    discard, is file by file: it is not atomic across storages. In it,
    ``pipe_file`` and ``put_file`` write through ``open``, so that their files
    join it. ``open_async`` takes no part in it, as in fsspec, whose
    ``open_async`` knows no transaction: its files are the inner filesystem's
    own, written as they close. Neither does a copy between two storages,
    which streams through ``open_async``.

    The router has no ``storage_profile``: each storage has its own,
    `storage_profile_of` tells it. Its only limit is ``transfer_limit``, the
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
        self._backends: dict[Hashable, AsyncFileSystem] = {}

    # What a subclass implements.

    @abstractmethod
    def _route(self, path: str) -> tuple[AsyncFileSystem, str] | None:
        """The inner filesystem of a path, and its path there, or None above them all.

        ``path`` is the router's, without the protocol. A trailing "/" is kept,
        as it means a folder to a copy or a glob: the router puts it back on
        the inner path if ``_route`` drops it. So may glob patterns: where
        the literal part routes, the pattern's must route to the same
        filesystem.

        It raises FileNotFoundError for a path no storage has, e.g. a missing
        volume. It may make the inner filesystem, with `_backend`.
        """

    @abstractmethod
    def _outer(self, fs: AsyncFileSystem, path: str) -> str:
        """The router's path of ``path``, a path of the inner filesystem ``fs``."""

    @abstractmethod
    async def _roots(self, path: str) -> list[dict[str, Any]]:
        """List a folder above the storages, as ``_ls`` with ``detail=True`` does.

        Each entry's name is the router's path. It raises FileNotFoundError
        for a folder that does not exist.
        """

    # What a subclass may override.

    async def _root_info(self, path: str) -> dict[str, Any]:
        """Describe a path above the storages, as ``_info`` does.

        ``path`` is the router's, without the protocol. Here, the root is a
        bare folder entry, and any other path the entry of its name in
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

    async def _server_copy(
        self, fs1: AsyncFileSystem, path1: str, fs2: AsyncFileSystem, path2: str
    ) -> bool:
        """Copy a file between two storages without the data passing through here.

        A subclass overrides it with the cloud's server-side copy, e.g. a copy
        from a URL. It returns False when the copy cannot be done so, e.g.
        between two clouds, and the file is then streamed. It raises when the
        copy fails otherwise. Here, it always returns False.
        """
        return False

    def _transfer(
        self,
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        *,
        copy_file: _Request,
    ) -> Operation[tuple[str, str], CpState] | None:
        """The recursive copy of ``path1`` in ``fs1`` to ``path2`` in ``fs2``, or None.

        The router knows no family, so the strategy is the subclass's to pick,
        as it knows its inner filesystems: e.g. `tree_transfer` between two
        `TreeFileSystem`s, given ``fs1``'s ``_ls`` and ``_info``, ``fs2``'s
        ``_info``, ``_makedirs`` and ``_rm_file``, its profile from
        `storage_profile_of`, and ``copy_file``. ``copy_file`` holds a permit
        of ``transfer_limit``, so the strategy's ``concurrency_limit`` must be
        another semaphore, e.g. ``fs1.operation_limit``.

        The operation is returned not yet started. The paths are the inner
        filesystems' own. Here, it returns None: the copy is fsspec's own, a
        file at a time.
        """
        return None

    def _backend(
        self, key: Hashable, make: Callable[[], AsyncFileSystem]
    ) -> AsyncFileSystem:
        """The inner filesystem of ``key``, made by ``make`` the first time.

        A subclass's ``_route`` makes its inner filesystems with it, e.g. one
        per volume, or per account and container. They are made with
        ``asynchronous=True`` and the router's ``loop``, so that their
        coroutines run where the router's do. ``close()`` closes them.
        """
        fs = self._backends.get(key)
        if fs is None:
            fs = self._backends[key] = make()
        return fs

    def close(self) -> None:
        """Close the inner filesystems made by `_backend`, and forget them.

        Each is closed by its own ``close()``, if it has one. A call after
        makes them again.
        """
        backends, self._backends = self._backends, {}
        for fs in backends.values():
            close = getattr(fs, "close", None)
            if callable(close):
                close()

    def storage_profile_of(self, path: str) -> StorageProfile:
        """The ``storage_profile`` of the storage of ``path``.

        Parameters
        ----------
        path : str
            A path of the router.

        Returns
        -------
        StorageProfile
            The inner filesystem's, or ``StorageProfile()`` if it has none, or
            the path is above the storages.
        """
        target = self._target(path)
        profile = getattr(target[0], "storage_profile", None) if target else None
        return profile if isinstance(profile, StorageProfile) else StorageProfile()

    # Routing.

    def _target(self, path: str) -> tuple[AsyncFileSystem, str] | None:
        """``_route`` of a path given to a method, refusing a sync filesystem.

        A trailing "/" of the path is kept on a non-empty inner path, as it
        means a folder to a copy or a glob: ``_route`` need not keep it.
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
            return False  # Above the storages, all are folders.

        fs, inner = target
        return await self._on(fs, inner, path, fs._isfile(inner))

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        fs1, inner1 = self._required(path1)
        fs2, inner2 = self._required(path2)

        if fs1 is fs2:
            await self._on(fs1, inner1, path1, fs1._cp_file(inner1, inner2, **kwargs))
        elif await self._on(fs1, inner1, path1, fs1._isdir(inner1)):
            # fsspec's copy gives folders too, which each filesystem makes.
            await self._on(fs2, inner2, path2, fs2._makedirs(inner2, exist_ok=True))
        else:
            copy_file = self._copy_file(fs1, fs2)
            await self._on(fs1, inner1, path1, copy_file(inner1, inner2))

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        fs1, inner1 = self._required(path1)
        fs2, inner2 = self._required(path2)

        if fs1 is fs2:
            await self._on(fs1, inner1, path1, fs1._mv_file(inner1, inner2, **kwargs))
        else:
            copy_file = self._copy_file(fs1, fs2, delete=True)
            await self._on(fs1, inner1, path1, copy_file(inner1, inner2))

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
            elif (
                recursive
                and maxdepth is None
                and (
                    op := self._transfer(
                        fs1, sources, fs2, inner2, copy_file=self._copy_file(fs1, fs2)
                    )
                )
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
        source folders left empty once all are. A source whose copy failed is
        kept.
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
            elif (
                recursive
                and maxdepth is None
                and (
                    op := self._transfer(
                        fs1,
                        sources,
                        fs2,
                        inner2,
                        copy_file=self._copy_file(fs1, fs2, delete=True),
                    )
                )
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

                # The folders left, now empty, as a move leaves none. A flat
                # storage's went with their files: none is found.
                with suppress(FileNotFoundError):
                    await self._on(
                        fs1, sources, outer, fs1._rm(sources, recursive=True)
                    )
            else:
                await super(AsyncRoutingFileSystem, self)._copy(
                    outer, path2, recursive, "raise", maxdepth, **kwargs
                )
                await self._rm(outer, recursive=recursive)

        await self._each(await self._grouped(path1), mv, "Moving", clashes)

    async def _transferred(
        self,
        op: Operation[tuple[str, str], CpState],
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        outer1: str | list[str],
        outer2: str,
        callback: Any,
        delete: bool = False,
    ) -> list[Exception]:
        """Run the copy `_transfer` gave between two storages: the errors.

        The call is the router's own, watched by the monitors as such.
        """
        call = Call(self, "mv" if delete else "copy", outer1, outer2)
        _, errors = await _collect_with_monitors(op, call, callback)
        for error in errors:
            error.add_note(
                f"Through {self!r}, from {fs1!r} as {path1!r} to {fs2!r} as {path2!r}."
            )

        return errors

    def _copy_file(
        self, fs1: AsyncFileSystem, fs2: AsyncFileSystem, delete: bool = False
    ) -> _Request:
        """The ``copy_file`` between two storages: `_server_copy`, or else streaming.

        With ``delete``, the source is deleted once copied, so that a move
        leaves no file in both places for longer than needed.
        """
        stream = streaming_copy(
            fs1.open_async,
            fs2.open_async,
            **(
                {"block_size": fs1.transfer_size}
                if hasattr(fs1, "transfer_size")
                else {}
            ),
        )

        async def copy_file(path1: str, path2: str) -> None:
            with routing(self):
                async with self.transfer_limit:
                    if not await self._server_copy(fs1, path1, fs2, path2):
                        await stream(path1, path2)

                if delete:
                    await fs1._rm_file(path1)

        return copy_file

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

    A storage needs no folder for the files put in it, and fsspec's sync
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
