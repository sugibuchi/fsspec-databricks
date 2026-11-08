# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive copy that copies the keys of a prefix as it lists them.

`flat_cp` is an alternative to fsspec's recursive ``copy`` for flat storages.
fsspec's lists every key first, then copies them. `flat_cp` copies each key as
soon as the listing gives it. The destination may be another storage, flat or
a tree: within one, it is the same storage's operations given twice.

This module depends on `_jobs`, and `jobs`, `operation` and `placement` of
`common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import CpState

from ..common.jobs import _Job
from ..common.operation import Operation
from ..common.placement import _Placing
from ._jobs import _expand, _FlatJobs

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Sequence

    from fsspec_alt_strategies.profile import StorageProfile

    from ..common.jobs import _Roots
    from ..common.types import _Ils, _Info, _Request

__all__ = ["CpState", "flat_cp", "flat_local_cp"]


def flat_cp(
    path1: str | Sequence[str],
    path2: str,
    *,
    ils: _Ils,
    info: _Info,
    cp_file: _Request,
    dst_info: _Info,
    dst_makedirs: _Request,
    dst_rm_file: _Request,
    same_storage: bool,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], CpState]:
    """Copy the keys under a prefix, copying as the listing goes on.

    It places the paths as fsspec's ``copy`` with ``recursive=True`` does, as
    `tree_cp` places them: into ``path2`` if it is an existing folder or ends
    with "/", and ``path1`` does not end with "/", otherwise onto it. A glob
    pattern copies each path it matches, relative to the matches' common
    prefix. A list copies each path it names or matches into ``path2``, under
    its name, as ``cp -r`` does. Existing files are overwritten.

    The keys are listed as `flat_find` lists them, and each is copied by one
    ``cp_file`` as soon as the listing gives it.

    The destination is described by the ``dst_`` operations and ``profile``,
    and may be flat or a tree. Within one storage, they are the source's own,
    and ``same_storage`` is True: a path copied onto itself, or under itself,
    is refused. Between two, ``cp_file`` copies from one to the other, e.g. the
    router's ``_cross_fs_cp_file``, and every byte may go through the caller. A path may then go to the same path, or under it.

    Unless the profile says it needs none, a folder is made at the destination
    before the first key copied into it, by one ``dst_makedirs``. A folder's
    marker, a key ending with "/", is no file to copy: its folder is made at
    the destination instead, so that a tree keeps it even when empty, unless
    the destination's folders are implicit.

    The copies run concurrently, so the destination's keys are created in
    another order than the source's. A storage listing in creation order
    shows it.

    Unlike fsspec's ``copy``, it keeps going past a failure, and reports each.
    A copy cancelled or failed leaves what was copied so far: it deletes
    nothing. A folder failing to be made is reported, and the keys into it
    are still copied, each reporting its own failure.

    It wins over fsspec's ``copy`` on large prefixes of flat storages: the
    copies start after the first page of the listing, not after the last.
    Between two storages, it wins over fsspec's way too, a ``get`` to local
    files then a ``put``: the data needs no local disk. It loses on sources
    with a hierarchical namespace, whose folders a listing by prefix does not
    see: `tree_cp` suits those.

    Once cancelled, it takes nothing more from the listing, and starts no new
    copy, but yields the outcomes of those sent. Closed, it waits for those
    without yielding them. Iterating it raises FileNotFoundError if ``path1``
    does not exist, or matches nothing, and ValueError if ``path2`` is inside
    it within one storage.

    Requests: one ``dst_info`` for ``path2``, unless it ends with "/" or
    ``path1`` is a pattern or a list. Patterns, or a list, add the requests
    of their `flat_glob`. The pages of one ``ils`` per folder copied, one
    after another, one ``cp_file`` per key, and one ``dst_makedirs`` per
    destination folder with keys or a marker. A destination refusing to
    overwrite costs a ``dst_info``, a ``dst_rm_file`` and a ``cp_file`` more
    per file it refused. Each request but the listing holds a permit of
    ``concurrency_limit`` while it runs, a ``cp_file`` between two storages
    too.

    The profile's ``parents_required=False``, or ``implicit_folders=True``,
    saves the ``dst_makedirs`` of each folder with keys, as their copies
    create it. ``implicit_folders=True`` saves those of the markers too.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to copy, a glob pattern, or a list of them, as
        ``ils`` takes paths.
    path2 : str
        Where to copy it, as above.
    ils : callable
        Lists every key starting with a prefix, called as ``ils(prefix)``: an
        async iterator of entries as fsspec's ``_ls`` gives them, in order of
        their names, each key once, as an object store lists them.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    cp_file : callable
        Copies a file to the destination, called as
        ``cp_file(source, destination)``: e.g. the filesystem's ``_cp_file``.
    dst_info : callable
        Describes a destination path, called as ``dst_info(path)``.
    dst_makedirs : callable
        Creates a destination folder and its parents, called as
        ``dst_makedirs(path, exist_ok=True)``. On a flat destination, it may
        do nothing.
    dst_rm_file : callable
        Deletes a destination file, called as ``dst_rm_file(path)``. Only a
        storage refusing to overwrite needs it.
    same_storage : bool
        Whether the destination is the source's storage.
    profile : StorageProfile
        What the destination does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request but the listing while it runs, on either
        storage. Share one to limit several operations, or files, together.

    Returns
    -------
    Operation
        Yielding ``(source, destination)`` for each key copied, or an error.
        Its state is a `CpState`.

    See Also
    --------
    tree_cp : The same, for storages listing one folder per request.
    flat_mv : The same, deleting the sources as it goes.

    Examples
    --------
    >>> cp = flat_cp(
    ...     "data/2024",
    ...     "backup/",
    ...     ils=my_ils,
    ...     info=fs._info,
    ...     cp_file=fs._cp_file,
    ...     dst_info=fs._info,
    ...     dst_makedirs=fs._makedirs,
    ...     dst_rm_file=fs._rm_file,
    ...     same_storage=True,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async for copied in cp:
    ...     if isinstance(copied, Exception):
    ...         print("failed:", copied)
    """
    return Operation(
        lambda op: _Copy(
            path1,
            path2,
            ils=ils,
            info=info,
            cp_file=cp_file,
            destination_info=dst_info,
            makedirs=dst_makedirs,
            rm_file=dst_rm_file,
            same_storage=same_storage,
            profile=profile,
            concurrency_limit=concurrency_limit,
            op=op,
        ).outcomes(),
        CpState(),
    )


def flat_local_cp(
    path1: str | Sequence[str],
    path2: str,
    *,
    ils: _Ils,
    info: _Info,
    cp_file: _Request,
    makedirs: _Request,
    rm_file: _Request,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], CpState]:
    """Copy the keys under a prefix within one storage, as `flat_cp` does.

    It is `flat_cp` with the destination's operations the source's own, and
    ``same_storage=True``: a path copied onto itself, or under itself, is
    refused.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to copy, a glob pattern, or a list of them.
    path2 : str
        Where to copy it, as with `flat_cp`.
    ils, info : callable
        The storage's operations, as with `flat_cp`.
    cp_file : callable
        Copies a file within the storage, called as
        ``cp_file(source, destination)``: e.g. the filesystem's ``_cp_file``.
    makedirs, rm_file : callable
        The storage's operations, as `flat_cp`'s ``dst_makedirs`` and
        ``dst_rm_file``.
    profile : StorageProfile
        What the storage does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request but the listing while it runs.

    Returns
    -------
    Operation
        As `flat_cp`'s.

    See Also
    --------
    flat_cp : The same, to another storage too.
    """
    return flat_cp(
        path1,
        path2,
        ils=ils,
        info=info,
        cp_file=cp_file,
        dst_info=info,
        dst_makedirs=makedirs,
        dst_rm_file=rm_file,
        same_storage=True,
        profile=profile,
        concurrency_limit=concurrency_limit,
    )


class _CopyOrMove(_Placing, _FlatJobs):
    """The jobs of a copy or a move: a folder made where needed, and one per key.

    A key's job waits for its destination folder, when the destination needs
    it. The keys of one folder come apart in the listing, so the folders made
    are remembered, and each is made once. A subclass says how a key goes, in
    `transfer_file`.
    """

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        ils: _Ils,
        info: _Info,
        destination_info: _Info,
        makedirs: _Request,
        same_storage: bool,
        profile: StorageProfile,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[Any, Any],
    ) -> None:
        super().__init__(ils, info, concurrency_limit, op)

        self.path1 = path1
        self.path2 = path2

        # ils and info are the source's. destination_info, makedirs and the
        # profile are the destination's.
        self.makedirs = makedirs
        self._placing(destination_info, same_storage=same_storage)

        # Unknown, folders are made first. Implicit folders need none.
        self.implicit_folders = bool(profile.implicit_folders)
        self.parents_required = (
            profile.parents_required is not False and not self.implicit_folders
        )

        self.files_found = 0
        self.files_done = 0

        # The destination folders made, or tried: their keys need not wait.
        self.made: set[str] = set()

        # The copies waiting for their destination folder, by that folder.
        self.after_mkdir: dict[str, list[_Job]] = {}

    def state(self) -> CpState:
        return CpState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_copied=self.files_done,
        )

    async def expand(self, pattern: str | Sequence[str]) -> _Roots:
        return await _expand(self.ils, self.info, pattern, self.concurrency_limit)

    def add(self, entry: dict[str, Any]) -> None:
        name = entry["name"]
        if name.endswith("/"):
            # A folder's marker: a tree destination keeps the folder.
            if not self.implicit_folders:
                self.queue(self.target(name.rstrip("/")), [], always=True)
            return

        self.files_found += 1
        target = self.target(name)
        self.queue(target.rpartition("/")[0], [_Job("copy", name, target)])

    def queue(self, folder: str, copies: list[_Job], *, always: bool = False) -> None:
        """Queue copies into a destination folder, made first if needed."""
        if not folder or folder in self.made or not (always or self.parents_required):
            self.waiting.extend(copies)
            return

        if folder not in self.after_mkdir:
            self.waiting.append(_Job("mkdir", "", folder))
        self.after_mkdir.setdefault(folder, []).extend(copies)

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "mkdir":
            return self.makedirs(job.destination, exist_ok=True)
        return self.transfer_file(job.source, job.destination)

    @abstractmethod
    async def transfer_file(self, source: str, destination: str) -> None:
        """Copy or move a key. An error raised notes its paths, as it is yielded."""

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            self.made.add(job.destination)
            self.waiting.extend(self.after_mkdir.pop(job.destination))
            return []

        self.files_done += 1
        return [(job.source, job.destination)]

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        if job.kind == "mkdir":
            # The copies still go: each then reports its own failure, if any.
            self.made.add(job.destination)
            self.waiting.extend(self.after_mkdir.pop(job.destination))
            error.add_note(f"Creating the folder {job.destination!r} failed.")

        # A key's error is noted where it happened, in transfer_file.
        return [error]


class _Copy(_CopyOrMove):
    """The jobs of one `flat_cp`: a ``cp_file`` per key."""

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        cp_file: _Request,
        rm_file: _Request,
        **kwargs: Any,
    ) -> None:
        super().__init__(path1, path2, **kwargs)
        self.cp_file = cp_file
        self.rm_file = rm_file

    async def transfer_file(self, source: str, destination: str) -> None:
        try:
            try:
                await self.cp_file(source, destination)
            except FileExistsError:
                # The destination refuses to overwrite. A folder there is no
                # file to replace. The job holds its permit already.
                entry = await self.destination_info(destination)
                if entry["type"] == "directory":
                    raise

                await self.rm_file(destination)
                await self.cp_file(source, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise
