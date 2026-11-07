# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A filesystem routing each path to the filesystem of its storage.

`AsyncRoutingFileSystem` is the base of filesystems spanning several storages
under one namespace, e.g. a cloud's volumes or containers, each with its own
filesystem. It knows no family: each inner filesystem runs its own strategies,
and a copy across two storages is the source's ``_transfer``, whatever its
family.

This module depends on `streams`, and on `common`: `filesystem`, `operation`,
`paths` and `types`.
"""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from contextlib import aclosing, suppress
from glob import has_magic
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem, sync

from fsspec_alt_strategies_common.monitor import Call, Monitored, routing
from fsspec_alt_strategies_common.profile import StorageProfile

from .common.filesystem import (
    _AbstractFileSystemMeta,
    _copy_errors,
    _plain,
    _raise,
)
from .common.operation import _collected
from .common.paths import _trim
from .streams import streaming_copy

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        Awaitable,
        Callable,
        Hashable,
        Sequence,
    )

    from .common.types import _Request

__all__ = ["AsyncRoutingFileSystem"]

_Group = tuple[AsyncFileSystem, list[str], list[str]]
"""An inner filesystem, the inner paths given to it, and the router's paths of them."""


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

    The primitives, e.g. ``_info``, ``_cat_file`` and ``_open``, call the
    inner filesystem's own. The recursive methods, e.g. ``_find``, ``_rm`` and
    ``_copy``, call the inner filesystem's own too, so that its strategies
    run. Several paths, e.g. of one ``rm``, are grouped by storage, each group
    sent at once. A failure raised by an inner filesystem gets a note naming
    the router's path, and its inner one.

    Above the storages, ``ls`` and ``info`` answer from ``_roots``. ``find``,
    ``du`` and ``walk`` are fsspec's own walk, a folder after another, handing
    each storage's folder to its filesystem. A glob whose literal prefix
    routes goes whole to that filesystem. Another lists the prefix's roots, as
    fsspec's ``glob`` does. A change above the storages, e.g. ``rm`` of a
    catalog, raises PermissionError.

    A copy or move between two paths of one storage is the inner filesystem's
    own: ``_same_storage`` names that filesystem, and both paths as its own. Between two storages, it is the
    source's ``_transfer``, of a `TreeFileSystem` or a `FlatFileSystem`, given
    a ``copy_file`` trying ``_server_copy``, then streaming the data. An inner
    filesystem without ``_transfer`` gets fsspec's own copy instead, a file at
    a time. A move between two storages deletes each source file once its
    copy succeeds, then the source folders left empty.

    Inner filesystems are async, on the router's loop: a sync one raises
    TypeError, and is to be wrapped, e.g. in a `SyncTreeWrapper`. A subclass
    makes them when first needed, with `_backend`, so that ``close()`` closes
    them. Pickling keeps the constructor's arguments, as fsspec's own does, so
    a copy makes its inner filesystems again.

    One transaction is the router's: ``open`` passes ``autocommit`` on, and
    fsspec adds each file written to the router's transaction. Its commit, or
    discard, is file by file: it is not atomic across storages. ``open_async``
    takes no part in it, as in fsspec, whose ``open_async`` knows no
    transaction: its files are the inner filesystem's own, written as they
    close.

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
        and must be kept in the inner path, as it means a folder to a copy or
        a glob. So may glob patterns: where the literal part routes, the
        pattern's must route to the same filesystem.

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

    def _same_storage(
        self,
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
    ) -> tuple[AsyncFileSystem, str | list[str], str] | None:
        """The one filesystem copying or moving ``path1`` to ``path2`` itself, or None.

        ``path1`` is ``fs1``'s inner path, or a list of them, and ``path2``
        ``fs2``'s. It returns the filesystem doing the copy or move with its
        own ``_copy`` or ``_mv``, and both paths as that filesystem's, the
        first in the shape given. None means two storages: the copy is a
        transfer.

        Here, it is only so for the same filesystem, whose paths are its own
        already. A subclass whose two filesystems share a storage, e.g. one
        client's, returns one of them, with the other's path mapped to it.
        """
        return (fs1, path1, path2) if fs1 is fs2 else None

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
        """``_route`` of a path given to a method, refusing a sync filesystem."""
        target = self._route(_plain(self, path))
        if target is not None and not getattr(target[0], "async_impl", False):
            raise TypeError(
                f"{target[0]!r} is not async: a router passes its calls on to async "
                "filesystems only. Wrap it, e.g. in a SyncTreeWrapper."
            )
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
            target = self._pattern_target(_plain(self, p))
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
        base = _trim(_plain(self, destination))
        for path in sorted(paths, key=lambda p: _trim(_plain(self, p))):
            if has_magic(path):
                continue
            name = _trim(_plain(self, path)).rpartition("/")[2]
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
        name = self._strip_protocol(path)
        if name in ("", self.root_marker):
            return {"name": name, "size": 0, "type": "directory"}
        for entry in await self._roots(self._parent(name)):
            if entry["name"].rstrip("/") == name:
                return entry
        raise FileNotFoundError(name)

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
        await self._forward(path, "_pipe_file", value, **kwargs)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        await self._forward(rpath, "_get_file", lpath, **kwargs)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        fs, inner = self._required(rpath)
        await self._on(fs, inner, rpath, fs._put_file(lpath, inner, **kwargs))

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
        if same := self._same_storage(fs1, inner1, fs2, inner2):
            fs, p1, p2 = same
            await self._on(fs, p1, path1, fs._cp_file(p1, p2, **kwargs))
        elif await self._on(fs1, inner1, path1, fs1._isdir(inner1)):
            # fsspec's copy gives folders too, which each filesystem makes.
            await self._on(fs2, inner2, path2, fs2._makedirs(inner2, exist_ok=True))
        else:
            copy_file = self._copy_file(fs1, fs2)
            await self._on(fs1, inner1, path1, copy_file(inner1, inner2))

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        fs1, inner1 = self._required(path1)
        fs2, inner2 = self._required(path2)
        if same := self._same_storage(fs1, inner1, fs2, inner2):
            fs, p1, p2 = same
            await self._on(fs, p1, path1, fs._mv_file(p1, p2, **kwargs))
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
        target = self._pattern_target(_plain(self, path))
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
            self._pattern_target(_plain(self, path)) if isinstance(path, str) else None
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
            if same := self._same_storage(fs1, sources, fs2, inner2):
                fs, p1, p2 = same
                await self._on(
                    fs,
                    p1,
                    outers,
                    fs._copy(
                        p1,
                        p2,
                        recursive,
                        on_error,
                        maxdepth,
                        batch_size,
                        **kwargs,
                    ),
                )
            elif recursive and maxdepth is None and hasattr(fs1, "_transfer"):
                errors = await self._transferred(
                    fs1, sources, fs2, inner2, outers, path2, callback
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
            if same := self._same_storage(fs1, sources, fs2, inner2):
                fs, p1, p2 = same
                own = getattr(fs, "_mv", None)
                if own is not None:
                    await self._on(
                        fs,
                        p1,
                        outer,
                        own(p1, p2, recursive=recursive, maxdepth=maxdepth, **kwargs),
                    )
                    return
                # A plain fsspec filesystem has no async mv: its sync one's way.
                await self._on(
                    fs,
                    p1,
                    outer,
                    fs._copy(p1, p2, recursive, "raise", maxdepth, **kwargs),
                )
                await self._on(fs, p1, outer, fs._rm(p1, recursive=recursive))
            elif recursive and maxdepth is None and hasattr(fs1, "_transfer"):
                errors = await self._transferred(
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
        fs1: AsyncFileSystem,
        path1: str | list[str],
        fs2: AsyncFileSystem,
        path2: str,
        outer1: str | list[str],
        outer2: str,
        callback: Any,
        delete: bool = False,
    ) -> list[Exception]:
        """Copy between two storages with the source's ``_transfer``: the errors.

        The call is the router's own, watched by the monitors as such.
        """
        op = fs1._transfer(  # type: ignore[attr-defined]
            path1, fs2, path2, copy_file=self._copy_file(fs1, fs2, delete=delete)
        )
        call = Call(self, "mv" if delete else "copy", outer1, outer2)
        _, errors = await _collected(op, call, callback)
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
