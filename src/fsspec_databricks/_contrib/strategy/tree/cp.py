# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive copy that walks the tree and copies while it walks.

`_tree_cp` is an alternative to fsspec's recursive ``copy``. fsspec's lists the
whole tree first, then copies each path. `_tree_cp` copies each directory's files
as soon as the directory is listed. The destination may be another storage:
within one, it is the same filesystem given twice.

This module depends on `_jobs`, and `jobs` and `paths` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from ..common.jobs import _CopyJobs, _Job, _transfer_of
from ..common.paths import _check_maxdepth
from ._jobs import _TreeJobs

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from fsspec_alt_strategies.states import CpState

    from ..common.jobs import _Transfer
    from ..common.operation import Operation
    from .walk import _Listing

__all__: list[str] = []


def _tree_cp(
    src_fs: Any,
    dst_fs: Any,
    path1: str | Sequence[str],
    path2: str,
    *,
    cp_entry: _Transfer | None = None,
    same_storage: bool | None = None,
    maxdepth: int | None = None,
) -> Operation[tuple[str, str], CpState]:
    """Copy a directory and everything in it, copying while the tree is walked.

    It places the paths as fsspec's ``copy`` with ``recursive=True`` does. If
    ``path2`` is an existing directory, or ends with "/", and ``path1`` does not
    end with "/", ``path1`` goes into it: ``path1/x`` becomes
    ``path2/<name of path1>/x``. Otherwise ``path1`` becomes ``path2``:
    ``path1/x`` becomes ``path2/x``. A file is copied the same way. Existing
    files are overwritten, as fsspec's ``copy`` does.

    A glob pattern copies each path it matches, and all in it, placed as
    fsspec's ``copy`` places them. Their paths below the matches' common
    prefix are kept under ``path2``, but a single file matched goes into
    ``path2``. The pattern is matched as `_tree_glob` matches it, then the
    directories matched are walked at once.

    A list of paths and patterns copies each path it names or matches into
    ``path2``, under its name, with all in it. That is how ``cp -r`` places
    them. fsspec's ``copy`` places the files named as this does. It also
    flattens the directories' trees, though: each file in them goes straight into
    ``path2``. Two paths of the same name would go to the same place. The
    first, in path order, is copied, and each other yields a FileExistsError
    instead, as GNU's ``cp`` reports it. fsspec's ``copy`` overwrites one
    with the other. A path inside another one in the list is copied with that
    one only.

    The tree is walked as `_tree_walk` walks it. Each directory's files are copied
    as soon as the directory is listed, each by a ``cp_entry``. A directory is
    created at the destination before files are copied into it, and for each
    empty directory.

    With ``maxdepth``, it copies the files fsspec's ``copy`` copies with it:
    those at most ``maxdepth`` levels below ``path1``, or below a pattern's
    matches one level less. Directories deeper are not listed. As fsspec's, it
    then makes no empty directory. Unlike fsspec's, it places the files as
    without ``maxdepth``: fsspec places them by the common prefix of the files
    found, which moves them when all are in one subdirectory.

    The destination is described by ``dst_fs`` and its ``storage_profile``,
    unknown if it has none. Within one storage, ``dst_fs`` is ``src_fs``, and
    ``same_storage`` is True: a path copied onto itself, or under itself, is
    refused. Between two, ``cp_entry`` copies from one to the other, e.g. the
    router's ``_cross_fs_cp_entry``, and every byte may go through the caller. A
    path may then go to the same path, or under it. The copies run
    concurrently, so the destination's paths are created in another order than
    the source's. A storage listing in creation order shows it.

    Unlike fsspec's ``copy``, it keeps going past a failure, and reports each.
    A copy cancelled or failed leaves what was copied so far: it deletes
    nothing.

    It wins over fsspec's ``copy`` on storages that list one directory per
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
    their `_tree_glob`. One recursive ``_ils`` per root, or one ``_ils`` per
    directory with ``maxdepth``, one ``cp_entry`` per file, and
    one ``dst_makedirs`` per destination directory with files or empty. A
    destination refusing to overwrite costs a ``dst_info``, a ``dst_rm_file``
    and a ``cp_entry`` more per file it refused. The ``dst_info`` checks it is
    a file, as ``dst_rm_file`` is given only paths known to be files. Each
    request holds a permit of the source's ``operation_limit`` while it runs,
    a ``cp_entry`` between two storages too.

    The destination's profile's ``parents_required=False``, or
    ``implicit_directories=True``, saves the ``dst_makedirs`` of each directory with
    files, as their copies create it.

    The primitives are called above by what they do: ``_ils`` and ``info`` are
    the source's ``_ils`` and ``_info``, ``dst_info``, ``dst_makedirs`` and
    ``dst_rm_file`` the destination's ``_info``, ``_makedirs`` and
    ``_rm_file``.

    ``src_fs`` is the filesystem copied from, and ``dst_fs`` the one copied
    to, ``src_fs`` again within one storage. ``path1`` is a directory, a file, a
    glob pattern, or a list of them, as the source's ``_ils`` takes paths.
    ``cp_entry(entry, destination)`` copies a file: the source's ``_cp_file_entry``
    by default, within one storage. ``same_storage`` is whether ``dst_fs`` is
    ``src_fs``, unless given. ``maxdepth`` of 1 copies only the files in
    ``path1``.

    The operation yields ``(source, destination)`` for each file copied, or an
    error, and its state is a `CpState`. The call raises ValueError if
    ``maxdepth`` is less than 1, or ``cp_entry`` is missing between two
    storages. `_tree_mv` does the same, deleting the source as it goes.
    """
    _check_maxdepth(maxdepth)
    cp_entry, same_storage = _transfer_of(
        src_fs, dst_fs, cp_entry, same_storage, "cp_entry"
    )
    return _Copy.operation(
        fs=src_fs,
        dst_fs=dst_fs,
        path1=path1,
        path2=path2,
        same_storage=same_storage,
        maxdepth=maxdepth,
        cp_entry=cp_entry,
    )


class _CopyOrMove(_CopyJobs, _TreeJobs):
    """The jobs of a copy or a move: a directory made where needed, and one per file.

    The tree walked is the source's, ``fs``. A file's job waits for its
    destination directory, when the storage needs it. A subclass says how a file
    goes, in `transfer_file`.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # The copies waiting for their destination directory, by that directory.
        self.after_mkdir: dict[str, list[_Job]] = {}

        # The destination directories made, or tried: a directory listed in several
        # parts is made once, and its later copies need not wait.
        self.made: set[str] = set()

    def file_root(self, entry: dict[str, Any]) -> None:
        self.files_found += 1
        path = entry["name"]
        target = self.target(path)
        job = _Job("copy", entry, target)
        self.queue(target.rpartition("/")[0], "", [job])

    def add(self, listing: _Listing) -> list[Any] | None:
        copies = []
        for entry in listing.entries:
            if entry["type"] != "directory":
                name = entry["name"].rstrip("/")
                self.files_found += 1
                # The entry goes with its copy, so that cp_entry need not ask it.
                copies.append(_Job("copy", {**entry, "name": name}, self.target(name)))

        if listing.first and listing.last and not listing.entries:
            # An empty directory is made, whatever the storage. With maxdepth,
            # fsspec's copy leaves out directories, and copies files only.
            if self.maxdepth is not None:
                return None
            self.queue(
                self.target(listing.directory), listing.directory, [], always=True
            )
        elif copies:
            # A directory holding only directories needs no mkdir: theirs create it.
            self.queue(self.target(listing.directory), listing.directory, copies)
        return None

    def queue(
        self, directory: str, source: str, copies: list[_Job], *, always: bool = False
    ) -> None:
        """Queue copies into a destination directory, made first if needed."""
        if (
            not directory
            or directory in self.made
            or not (always or self.parents_required)
        ):
            self.waiting.extend(copies)
            return

        if directory not in self.after_mkdir:
            self.waiting.append(_Job("mkdir", {"name": source}, directory))
        self.after_mkdir.setdefault(directory, []).extend(copies)

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "mkdir":
            return self.dst_fs._makedirs(job.destination, exist_ok=True)
        return self.transfer_file(job.source, job.destination)

    @abstractmethod
    async def transfer_file(self, entry: dict[str, Any], destination: str) -> None:
        """Copy or move a file, by its entry. An error raised notes its paths, as it is yielded."""

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            self.made.add(job.destination)
            self.waiting.extend(self.after_mkdir.pop(job.destination))
            return []
        self.files_done += 1
        return [(job.path, job.destination)]

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        # A file's error is noted where it happened, in transfer_file.
        if job.kind == "mkdir":
            # Its copies, and those of the directory's later parts, are not sent:
            # each would fail as it did.
            error.add_note(f"Creating the directory {job.destination!r} failed.")
        return [error]


class _Copy(_CopyOrMove):
    """The jobs of one `_tree_cp`: a ``cp_entry`` per file."""

    def __init__(self, *, cp_entry: _Transfer, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.cp_entry = cp_entry

    async def transfer_file(self, entry: dict[str, Any], destination: str) -> None:
        await self.copy_file(self.cp_entry, entry, destination)
