# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive copy that walks the tree and copies while it walks.

`tree_cp` is an alternative to fsspec's recursive ``copy``. fsspec's lists the
whole tree first, then copies each path. `tree_cp` copies each folder's files
as soon as the folder is listed. The destination may be another storage:
within one, it is the same storage's operations given twice.

This module depends on `_jobs`, and `jobs`, `operation`, `paths` and
`placement` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import CpState

from ..common.jobs import _Job
from ..common.operation import Operation
from ..common.paths import _check_maxdepth
from ..common.placement import _Placing
from ._jobs import _TreeJobs

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Sequence

    from fsspec_alt_strategies.profile import StorageProfile

    from ..common.jobs import _Roots
    from ..common.types import _Ils, _Info, _Request
    from .walk import _Listing

__all__ = ["CpState", "tree_cp", "tree_local_cp"]


def tree_cp(
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
    maxdepth: int | None = None,
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

    With ``maxdepth``, it copies the files fsspec's ``copy`` copies with it:
    those at most ``maxdepth`` levels below ``path1``, or below a pattern's
    matches one level less. Folders deeper are not listed. As fsspec's, it
    then makes no empty folder. Unlike fsspec's, it places the files as
    without ``maxdepth``: fsspec places them by the common prefix of the files
    found, which moves them when all are in one subfolder.

    The destination is described by the ``dst_`` operations and ``profile``.
    Within one storage, they are the source's own, and ``same_storage`` is
    True: a path copied onto itself, or under itself, is refused. Between two,
    ``cp_file`` copies from one to the other, e.g. the router's
    ``_cross_fs_cp_file``, and every byte may go through the caller.
    A path may then go to the same path, or under it.

    The copies run concurrently, so the destination's paths are created in
    another order than the source's. A storage listing in creation order
    shows it.

    Unlike fsspec's ``copy``, it keeps going past a failure, and reports each.
    A copy cancelled or failed leaves what was copied so far: it deletes
    nothing.

    It wins over fsspec's ``copy`` on storages that list one folder per
    request. The copies then start after the first listing, not after the
    whole tree is listed. Between two storages, it wins over fsspec's way too,
    a ``get`` to local files then a ``put``: the data needs no local disk.

    Once cancelled, it starts no new listing or copy, and yields the outcomes
    of the copies already sent. Closed, it waits for those copies without
    yielding them. Iterating it raises FileNotFoundError if ``path1`` does not
    exist, or matches nothing, and ValueError if ``path2`` is inside it within
    one storage.

    Requests: one ``dst_info`` for ``path2``, unless it ends with "/" or
    ``path1`` is a pattern or a list. Patterns, or a list, add the requests of
    their `tree_glob`. One ``ils`` per folder, one ``cp_file`` per file, and
    one ``dst_makedirs`` per destination folder with files or empty. A
    destination refusing to overwrite costs a ``dst_info``, a ``dst_rm_file``
    and a ``cp_file`` more per file it refused. The ``dst_info`` checks it is
    a file, as ``dst_rm_file`` is given only paths known to be files. Each
    request holds a permit of ``concurrency_limit`` while it runs, a
    ``cp_file`` between two storages too.

    The profile's ``parents_required=False``, or ``implicit_folders=True``,
    saves the ``dst_makedirs`` of each folder with files, as their copies
    create it.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to copy, a glob pattern, or a list of them, as
        ``ils`` takes paths.
    path2 : str
        Where to copy it, as above.
    ils : callable
        Lists a folder, called as ``ils(path)``: an async iterator of its
        entries, as fsspec's ``_ls`` gives them, e.g. the filesystem's
        ``_ils``.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    cp_file : callable
        Copies a file to the destination, called as
        ``cp_file(source, destination)``: e.g. the filesystem's ``_cp_file``.
    dst_info : callable
        Describes a destination path, called as ``dst_info(path)``: e.g. the
        filesystem's ``_info``.
    dst_makedirs : callable
        Creates a destination folder and its parents, called as
        ``dst_makedirs(path, exist_ok=True)``: e.g. the filesystem's
        ``_makedirs``.
    dst_rm_file : callable
        Deletes a destination file, called as ``dst_rm_file(path)``: e.g. the
        filesystem's ``_rm_file``. Only a storage refusing to overwrite needs
        it.
    same_storage : bool
        Whether the destination is the source's storage.
    profile : StorageProfile
        What the destination does: ``StorageProfile()`` if it is unknown.
    maxdepth : int, optional
        The deepest level to copy: 1 copies only the files in ``path1``. No
        limit by default.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs, on either storage. Share one to
        limit several operations, or files, together.

    Returns
    -------
    Operation
        Yielding ``(source, destination)`` for each file copied, or an error.
        Its state is a `CpState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1.

    See Also
    --------
    tree_walk : The walk under it.
    tree_mv : The same, deleting the source as it goes.

    Examples
    --------
    >>> cp = tree_cp(
    ...     "data/2024",
    ...     "backup/",
    ...     ils=fs._ils,
    ...     info=fs._info,
    ...     cp_file=fs._cp_file,
    ...     dst_info=fs._info,
    ...     dst_makedirs=fs._makedirs,
    ...     dst_rm_file=fs._rm_file,
    ...     same_storage=True,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with cp as op:
    ...     async for copied in op:
    ...         if isinstance(copied, Exception):
    ...             print("failed:", copied)
    >>> op.state.files_copied
    1234

    Between two storages, the data streamed through the caller:

    >>> cp = tree_cp(
    ...     "data/2024",
    ...     "backup/",
    ...     ils=src._ils,
    ...     info=src._info,
    ...     cp_file=my_streaming_copy,
    ...     dst_info=dst._info,
    ...     dst_makedirs=dst._makedirs,
    ...     dst_rm_file=dst._rm_file,
    ...     same_storage=False,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    """
    _check_maxdepth(maxdepth)
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
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
            op=op,
        ).outcomes(),
        CpState(),
    )


def tree_local_cp(
    path1: str | Sequence[str],
    path2: str,
    *,
    ils: _Ils,
    info: _Info,
    cp_file: _Request,
    makedirs: _Request,
    rm_file: _Request,
    profile: StorageProfile,
    maxdepth: int | None = None,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], CpState]:
    """Copy a folder and everything in it within one storage, as `tree_cp` does.

    It is `tree_cp` with the destination's operations the source's own, and
    ``same_storage=True``: a path copied onto itself, or under itself, is
    refused.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to copy, a glob pattern, or a list of them.
    path2 : str
        Where to copy it, as with `tree_cp`.
    ils, info : callable
        The storage's operations, as with `tree_cp`.
    cp_file : callable
        Copies a file within the storage, called as
        ``cp_file(source, destination)``: e.g. the filesystem's ``_cp_file``.
    makedirs, rm_file : callable
        The storage's operations, as `tree_cp`'s ``dst_makedirs`` and
        ``dst_rm_file``.
    profile : StorageProfile
        What the storage does: ``StorageProfile()`` if it is unknown.
    maxdepth : int, optional
        The deepest level to copy, as with `tree_cp`.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs.

    Returns
    -------
    Operation
        As `tree_cp`'s.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1.

    See Also
    --------
    tree_cp : The same, to another storage too.
    """
    return tree_cp(
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
        maxdepth=maxdepth,
        concurrency_limit=concurrency_limit,
    )


class _CopyOrMove(_Placing, _TreeJobs):
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
        ils: _Ils,
        info: _Info,
        destination_info: _Info,
        makedirs: _Request,
        same_storage: bool,
        profile: StorageProfile,
        maxdepth: int | None,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[Any, Any],
    ) -> None:
        super().__init__(ils, info, concurrency_limit, op, maxdepth)

        self.path1 = path1
        self.path2 = path2

        # ils and info are the source's. destination_info, makedirs and the
        # profile are the destination's.
        self.makedirs = makedirs

        # Unknown, folders are made first. Implicit folders need none.
        self.parents_required = (
            profile.parents_required is not False and not profile.implicit_folders
        )
        self.implicit_folders = bool(profile.implicit_folders)

        self._placing(destination_info, same_storage=same_storage)

        self.files_found = 0
        self.files_done = 0

        # The copies waiting for their destination folder, by that folder.
        self.after_mkdir: dict[str, list[_Job]] = {}

        # The destination folders made, or tried: a folder listed in several
        # parts is made once, and its later copies need not wait.
        self.made: set[str] = set()

    def state(self) -> Any:
        return CpState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_copied=self.files_done,
        )

    async def expand(self, pattern: str | Sequence[str]) -> _Roots:
        return await self.glob_roots(pattern)

    def file_root(self, path: str) -> None:
        self.files_found += 1
        target = self.target(path)
        self.queue(target.rpartition("/")[0], "", [_Job("copy", path, target)])

    def add(self, listing: _Listing) -> list[Any] | None:
        copies = []
        for entry in listing.entries:
            if entry["type"] != "directory":
                name = entry["name"].rstrip("/")
                self.files_found += 1
                copies.append(_Job("copy", name, self.target(name)))

        if listing.first and listing.last and not listing.entries:
            # An empty folder is made, whatever the storage. With maxdepth,
            # fsspec's copy leaves out folders, and copies files only.
            if self.maxdepth is not None:
                return None
            self.queue(self.target(listing.folder), listing.folder, [], always=True)
        elif copies:
            # A folder holding only folders needs no mkdir: theirs create it.
            self.queue(self.target(listing.folder), listing.folder, copies)
        return None

    def queue(
        self, folder: str, source: str, copies: list[_Job], *, always: bool = False
    ) -> None:
        """Queue copies into a destination folder, made first if needed."""
        if not folder or folder in self.made or not (always or self.parents_required):
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
            self.made.add(job.destination)
            self.waiting.extend(self.after_mkdir.pop(job.destination))
            return []
        self.files_done += 1
        return [(job.source, job.destination)]

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        # A file's error is noted where it happened, in transfer_file.
        if job.kind == "mkdir":
            # Its copies, and those of the folder's later parts, are not sent:
            # each would fail as it did.
            error.add_note(f"Creating the folder {job.destination!r} failed.")
        return [error]


class _Copy(_CopyOrMove):
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
                # The destination refuses to overwrite, which fsspec's copy
                # does. A folder there is no file to replace: rm_file takes
                # files only. The job holds its permit already.
                if (await self.destination_info(destination))["type"] == "directory":
                    raise

                await self.rm_file(destination)
                await self.cp_file(source, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise
