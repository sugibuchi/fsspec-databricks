# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive copy that walks the tree and copies while it walks.

`tree_cp` is an alternative to fsspec's recursive ``copy``. fsspec's lists the
whole tree first, then copies each path. `tree_cp` copies each folder's files
as soon as the folder is listed.

This module depends on `_jobs`, and `jobs`, `operation` and `placement` of
`common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies_common.states import CpState

from ..common.jobs import _Job
from ..common.operation import Operation
from ..common.placement import _Placing
from ._jobs import _expand, _TreeJobs

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Sequence

    from fsspec_alt_strategies_common.profile import StorageProfile

    from ..common.jobs import _Roots
    from ..common.types import _Info, _Ls, _Request
    from .walk import _Listing

__all__ = ["CpState", "tree_cp"]


def tree_cp(
    path1: str | Sequence[str],
    path2: str,
    *,
    ls: _Ls,
    info: _Info,
    cp_file: _Request,
    makedirs: _Request,
    rm_file: _Request,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], CpState]:
    """Copy a folder and everything in it, copying while the tree is walked.

    It places the paths as fsspec's ``copy`` with ``recursive=True`` does. If
    ``path2`` is an existing folder, or ends with "/", and ``path1`` does not
    end with "/", ``path1`` goes into it: ``path1/x`` becomes
    ``path2/<name of path1>/x``. Otherwise ``path1`` becomes ``path2``:
    ``path1/x`` becomes ``path2/x``. A file is copied the same way. Existing
    files are overwritten, as fsspec's ``copy`` does.

    A glob pattern copies each path it matches, and all in it, placed as
    fsspec's ``copy`` places them. Their paths below the matches' common
    prefix are kept under ``path2``, but a single file matched goes into
    ``path2``. The pattern is matched as `tree_glob` matches it, then the
    folders matched are walked at once.

    A list of paths and patterns copies each path it names or matches into
    ``path2``, under its name, with all in it. That is how ``cp -r`` places
    them. fsspec's ``copy`` places the files named as this does. It also
    flattens the folders' trees, though: each file in them goes straight into
    ``path2``. Two paths of the same name would go to the same place. The
    first, in path order, is copied, and each other yields a FileExistsError
    instead, as GNU's ``cp`` reports it. fsspec's ``copy`` overwrites one
    with the other. A path inside another one in the list is copied with that
    one only.

    The tree is walked as `tree_walk` walks it. Each folder's files are copied
    as soon as the folder is listed, each by a ``cp_file``. A folder is
    created at the destination before files are copied into it, and for each
    empty folder.

    Unlike fsspec's ``copy``, it keeps going past a failure, and reports each.
    A copy cancelled or failed leaves what was copied so far: it deletes
    nothing.

    It wins over fsspec's ``copy`` on storages that list one folder per
    request. The copies then start after the first listing, not after the
    whole tree is listed.

    Once cancelled, it starts no new listing or copy, and yields the outcomes
    of the copies already sent. Closed, it waits for those copies without
    yielding them. Iterating it raises FileNotFoundError if ``path1`` does not
    exist, or matches nothing, and ValueError if ``path2`` is inside it.

    Requests: one ``info`` for ``path2``, unless it ends with "/" or ``path1``
    is a pattern or a list. Patterns, or a list, add the requests of their
    `tree_glob`. One ``ls`` per folder, one ``cp_file`` per file, and one
    ``makedirs`` per destination folder with files or empty. A storage
    refusing to overwrite costs an ``rm_file`` and a ``cp_file`` more per file
    it refused. Each request holds a permit of ``concurrency_limit`` while it
    runs.

    The profile's ``parents_required=False``, or ``implicit_folders=True``,
    saves the ``makedirs`` of each folder with files, as their copies create
    it.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to copy, a glob pattern, or a list of them, as
        ``ls`` takes paths.
    path2 : str
        Where to copy it, as above.
    ls : callable
        Lists a folder, called as ``ls(path, detail=True)``: e.g. the
        filesystem's ``_ls``.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    cp_file : callable
        Copies a file, called as ``cp_file(source, destination)``: e.g. the
        filesystem's ``_cp_file``.
    makedirs : callable
        Creates a folder and its parents, called as
        ``makedirs(path, exist_ok=True)``: e.g. the filesystem's ``_makedirs``.
    rm_file : callable
        Deletes a file, called as ``rm_file(path)``: e.g. the filesystem's
        ``_rm_file``. Only a storage refusing to overwrite needs it.
    profile : StorageProfile
        What the storage does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs. Share one to limit several
        operations, or files, together.

    Returns
    -------
    Operation
        Yielding ``(source, destination)`` for each file copied, or an error.
        Its state is a `CpState`.

    See Also
    --------
    tree_walk : The walk under it.
    tree_mv : The same, deleting the source as it goes.

    Examples
    --------
    >>> cp = tree_cp(
    ...     "data/2024",
    ...     "backup/",
    ...     ls=fs._ls,
    ...     info=fs._info,
    ...     cp_file=fs._cp_file,
    ...     makedirs=fs._makedirs,
    ...     rm_file=fs._rm_file,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with cp as op:
    ...     async for copied in op:
    ...         if isinstance(copied, Exception):
    ...             print("failed:", copied)
    >>> op.state.files_copied
    1234
    """
    return Operation(
        lambda op: _Copy(
            path1,
            path2,
            ls=ls,
            info=info,
            cp_file=cp_file,
            makedirs=makedirs,
            rm_file=rm_file,
            profile=profile,
            concurrency_limit=concurrency_limit,
            op=op,
        ).outcomes(),
        CpState(),
    )


class _Transfer(_Placing, _TreeJobs):
    """The jobs of a copy or a move: a folder made where needed, and one per file.

    A file's job waits for its destination folder, when the storage needs it.
    A subclass says how a file goes, in `transfer_file`.
    """

    verb = "copy"
    doing = "Copying"

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        ls: _Ls,
        info: _Info,
        makedirs: _Request,
        profile: StorageProfile,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[Any, Any],
    ) -> None:
        super().__init__(ls, info, concurrency_limit, op)
        self.path1 = path1
        self.path2 = path2
        self.makedirs = makedirs
        # Unknown, folders are made first. Implicit folders need none.
        self.parents_required = (
            profile.parents_required is not False and not profile.implicit_folders
        )
        self.implicit_folders = bool(profile.implicit_folders)
        self._placing()
        self.files_found = 0
        self.files_done = 0
        # The copies waiting for their destination folder, by that folder.
        self.after_mkdir: dict[str, list[_Job]] = {}

    def state(self) -> Any:
        return CpState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_copied=self.files_done,
        )

    async def expand(self, pattern: str | Sequence[str]) -> _Roots:
        return await _expand(self.ls, self.info, pattern, self.concurrency_limit)

    def file_root(self, path: str) -> None:
        self.files_found += 1
        target = self.target(path)
        self.queue(target.rpartition("/")[0], "", [_Job("copy", path, target)])

    def add(self, listing: _Listing) -> None:
        copies = []
        for entry in listing.entries:
            if entry["type"] != "directory":
                name = entry["name"].rstrip("/")
                self.files_found += 1
                copies.append(_Job("copy", name, self.target(name)))
        if not listing.entries:
            # An empty folder is made, whatever the storage.
            self.queue(self.target(listing.folder), listing.folder, [], always=True)
        elif copies:
            # A folder holding only folders needs no mkdir: theirs create it.
            self.queue(self.target(listing.folder), listing.folder, copies)

    def queue(
        self, folder: str, source: str, copies: list[_Job], *, always: bool = False
    ) -> None:
        """Queue copies into a destination folder, made first if needed."""
        if not folder or not (always or self.parents_required):
            self.waiting.extend(copies)
            return
        if folder not in self.after_mkdir:
            self.waiting.append(_Job("mkdir", source, folder))
        self.after_mkdir.setdefault(folder, []).extend(copies)

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "mkdir":
            return self.makedirs(job.destination, exist_ok=True)
        return self.transfer_file(job.source, job.destination)

    @abstractmethod
    async def transfer_file(self, source: str, destination: str) -> None:
        """Copy or move a file. An error raised notes its paths, as it is yielded."""

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            self.waiting.extend(self.after_mkdir.pop(job.destination))
            return []
        self.files_done += 1
        return [(job.source, job.destination)]

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        # A file's error is noted where it happened, in transfer_file.
        if job.kind == "mkdir":
            error.add_note(f"Creating the folder {job.destination!r} failed.")
        return [error]


class _Copy(_Transfer):
    """The jobs of one `tree_cp`: a ``cp_file`` per file."""

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
                # The storage refuses to overwrite, which fsspec's copy does.
                await self.rm_file(destination)
                await self.cp_file(source, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise
