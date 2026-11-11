# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive move that walks the tree and moves while it walks.

`_tree_mv` is an alternative to fsspec's recursive ``mv``. fsspec's copies the
whole tree, then deletes it, each stage listing the tree first. `_tree_mv`
moves each directory's files as soon as the directory is listed, each by one
``mv_entry``, and deletes each source directory once it is empty.

Where the filesystem's ``_mv_tree`` moves a directory in one request, as a
storage's rename does, the profile says so. `_tree_mv` then moves each root
whole with it where it can, and walks only what it cannot move so.

This module depends on `cp`, and `jobs` and `paths` of `common`.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import MvState

from ..common.jobs import _Job, _transfer_of
from ..common.paths import _check_maxdepth
from .cp import _CopyOrMove

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from ..common.jobs import _Roots, _Transfer
    from ..common.operation import Operation
    from .walk import _Listing

__all__: list[str] = []


def _tree_mv(
    src_fs: Any,
    dst_fs: Any,
    path1: str | Sequence[str],
    path2: str,
    *,
    mv_entry: _Transfer | None = None,
    same_storage: bool | None = None,
    maxdepth: int | None = None,
) -> Operation[tuple[str, str], MvState]:
    """Move a directory and everything in it, moving while the tree is walked.

    It places the paths as fsspec's ``mv`` with ``recursive=True`` does. If
    ``path2`` is an existing directory, or ends with "/", and ``path1`` does not
    end with "/", ``path1`` goes into it: ``path1/x`` becomes
    ``path2/<name of path1>/x``. Otherwise ``path1`` becomes ``path2``:
    ``path1/x`` becomes ``path2/x``. A file moves the same way. Existing files
    are overwritten, as fsspec's ``mv`` does.

    A glob pattern moves each path it matches, and all in it, placed as
    fsspec's ``mv`` places them. Their paths below the matches' common
    prefix are kept under ``path2``, but a single file matched goes into
    ``path2``. The pattern is matched as `_tree_glob` matches it, then the
    directories matched are walked at once.

    A list of paths and patterns moves each path it names or matches into
    ``path2``, under its name, with all in it, as `_tree_cp` copies it. Unlike
    fsspec's ``mv``, it keeps the directories' trees. A path whose name another
    path takes first yields a FileExistsError, and stays where it is.

    The tree is walked as `_tree_walk` walks it. Each directory's files are
    moved as soon as the directory is listed, each by one ``mv_entry``. A
    source directory is deleted once everything in it is moved, so the deepest
    go first. A directory is created at the destination before files are moved
    into it, and for each empty directory.

    The destination is described by ``dst_fs`` and its ``storage_profile``,
    unknown if it has none. Within one storage, ``dst_fs`` is ``src_fs``, and
    ``same_storage`` is True. Between two, ``mv_entry`` copies a file to the
    other storage, then deletes it: e.g. the router's ``_cross_fs_mv_entry``. A
    path may then go to the same path, or under it, and nothing is moved whole.

    Unlike fsspec's ``mv``, it keeps going past a failure, and reports each. A
    file that failed to move stays where it was, and so do the directories above
    it. A storage refusing to overwrite yields a FileExistsError for each
    file already at its destination, as fsspec's ``mv`` raises one.

    It wins over fsspec's ``mv`` on storages that list one directory per request
    and rename nothing in one request. The moves then start after the first
    listing, not after the whole tree is listed twice.

    Within one storage, with the profile's ``renames_directories=True``,
    ``mv_tree`` moves a directory with all in it, in one request. Each
    directory root is then moved whole by one ``mv_tree``, where it can be, as
    a job among the files' moves. That wins by far on storages renaming a
    directory at once, such as Azure Data Lake Storage Gen2, HDFS or a local
    disk. A root goes whole only to a destination that does not exist, as a
    rename onto an existing path differs among storages. Into an existing
    directory, its contents merge, and its files are moved one by one. So are
    those of ``path1`` ending with "/", whose contents go onto ``path2``. A
    filesystem is one place renames work in: a storage where they stop, e.g.
    at containers, is several filesystems under a router. A root whose move
    fails yields the error, with its cause, and stays where it is. A root
    moved whole is yielded once, as ``(root, target)``: what is in it is
    neither listed nor yielded.

    With ``maxdepth``, it moves the files `_tree_cp` copies with it, and
    nothing is moved whole. A source directory is deleted once emptied, so those
    keeping a directory at the limit stay, and empty ones stay too. fsspec's
    ``mv`` copies as its ``copy`` does with ``maxdepth``, but then deletes the
    whole source tree, deeper files too. This keeps them.

    Once cancelled, it starts no new listing or move, and yields the outcomes
    of the moves already sent. Closed, it waits for those moves without
    yielding them, as their outcome would otherwise be unknown. Iterating it
    raises FileNotFoundError if ``path1`` does not exist, or matches nothing,
    and ValueError if ``path2`` is inside it within one storage.

    Requests: as `_tree_cp`, with one ``mv_entry`` per file instead of its
    ``cp_entry``, and one ``rm_dir`` per source directory. With
    ``renames_directories``, each directory root first costs a ``dst_info`` of
    its target, unless ``path2`` was found missing already. One moving whole
    then costs a ``dst_makedirs`` of its parent, and its ``mv_tree``: nothing
    more. Each request holds a permit of the source's ``operation_limit``
    while it runs.

    A destination's parent is made before any rename, as ``mv_entry`` and
    ``mv_tree`` are called only where the parent exists. The destination's
    profile's ``parents_required=False`` saves the ``dst_makedirs`` of each
    directory with files only where ``mv_entry`` writes, creating the
    directory: between two storages, or with ``renames_files=False``. Its
    ``implicit_directories=True`` saves those, and within one storage the
    ``rm_dir`` of each source directory too, as a directory goes with its last
    file. It alone saves the ``dst_makedirs`` before a directory is moved
    whole.

    The primitives are called above by what they do: ``_ils``, ``rm_dir`` and
    ``mv_tree`` are the source's ``_ils``, ``_rmdir`` and ``_mv_tree``, and
    ``dst_info`` and ``dst_makedirs`` the destination's ``_info`` and
    ``_makedirs``.

    ``src_fs`` is the filesystem moved from, and ``dst_fs`` the one moved to,
    ``src_fs`` again within one storage. ``path1`` is a directory, a file, a
    glob pattern, or a list of them, as the source's ``_ils`` takes paths.
    ``mv_entry(entry, destination)`` moves a file by its source entry, as its
    listing gave it: the source's ``_mv_file_entry`` by default, within one
    storage. A storage renaming a file in one request implements it so.
    ``same_storage`` is whether ``dst_fs`` is ``src_fs``, unless given.
    ``maxdepth`` of 1 moves only the files in ``path1``.

    The operation yields ``(source, destination)`` for each file and directory
    moved, or an error, and its state is an `MvState`. The call raises
    ValueError if ``maxdepth`` is less than 1, or ``mv_entry`` is missing
    between two storages. `_tree_cp` does the same, keeping the source.
    """
    _check_maxdepth(maxdepth)
    mv_entry, same_storage = _transfer_of(
        src_fs, dst_fs, mv_entry, same_storage, "mv_entry"
    )
    return _Move.operation(
        fs=src_fs,
        dst_fs=dst_fs,
        path1=path1,
        path2=path2,
        same_storage=same_storage,
        maxdepth=maxdepth,
        mv_entry=mv_entry,
    )


class _Move(_CopyOrMove):
    """The jobs of one `_tree_mv`: a ``mv_entry`` per file, deleting each directory emptied."""

    verb = "move"
    doing = "Moving"

    def __init__(self, *, mv_entry: _Transfer, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self.mv_entry = mv_entry
        self.renames_directories = False  # Until setup reads the profile.
        self.directories_moved = 0
        self.roots_renamed: int | None = None

    async def setup(self) -> None:
        await super().setup()

        # The profile is the destination's: only within one storage does it
        # tell how the source renames, or deletes its directories. A
        # directory moved whole would take what is deeper than maxdepth along.
        self.renames_directories = (
            self.same_storage
            and bool(self.profile.renames_directories)
            and self.maxdepth is None
        )
        self.implicit_directories = self.same_storage and self.implicit_directories
        # A rename makes no parent, as a write may: parents_required tells of
        # writes only. Within one storage, mv_entry may rename unless the
        # profile says it does not, so its directory is made first.
        self.parents_required = self.parents_required or (
            self.same_storage
            and not self.implicit_directories
            and self.profile.renames_files is not False
        )
        self.roots_renamed = 0 if self.renames_directories else None

    def state(self) -> MvState:
        return MvState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_moved=self.files_done,
            directories_moved=self.directories_moved,
            roots_renamed=self.roots_renamed,
        )

    async def prepare(self) -> _Roots | None:
        """Place the roots, and queue the move of each directory that can go whole: the rest are walked.

        A root can move whole to a target found missing. One whose target
        exists merges into it, so it is walked. One whose target could not be
        asked yields that error, with its cause, and stays where it is.
        """
        roots = await super().prepare()
        if roots is None or not self.renames_directories:
            return roots

        if isinstance(self.path1, str) and self.path1.endswith("/"):
            return roots  # Its contents go onto path2, among what is there.

        sources = list(roots.directories)
        whole = await asyncio.gather(*(self.whole(s) for s in sources))
        walked = []
        for root, outcome in zip(sources, whole, strict=True):
            if isinstance(outcome, Exception):
                self.found.append(outcome)
                continue
            if not outcome:
                walked.append(root)
                continue

            # A job each, as a file's move, after its parent is made: a rename
            # makes none, and mv_tree is not to make it.
            target = self.target(root["name"])
            parent = target.rpartition("/")[0]
            job = _Job("rename", root, target)
            self.queue(parent, "", [job], always=not self.implicit_directories)

        self.roots_renamed = 0
        return roots._replace(directories=walked)

    async def whole(self, root: dict[str, Any]) -> bool | Exception:
        """Whether a root can move whole, as its target is missing.

        An error asking is returned, noted: the root's, as its walk would only
        meet it again on each file.
        """
        source = root["name"]
        target = self.target(source)
        if target in self.missing:
            return True
        try:
            async with self.concurrency_limit:
                await self.dst_fs._info(target)
        except FileNotFoundError:
            return True
        except Exception as e:
            e.add_note(f"{self.doing} {source!r} to {target!r} failed.")
            return e
        else:
            return False

    def add(self, listing: _Listing) -> list[Any] | None:
        self.track(listing)
        super().add(listing)

        # All in it moved before its last part came. An empty directory goes once
        # made at the destination, in done.
        empty = listing.first and not listing.entries
        if listing.last and not empty and self.finished(listing.directory):
            return self.remove(listing.directory)
        return None

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "rmdir":
            return self.fs._rmdir(job.path)
        if job.kind == "rename":
            return self.fs._mv_tree(job.path, job.destination)
        return super().run(job)

    async def transfer_file(self, entry: dict[str, Any], destination: str) -> None:
        try:
            await self.mv_entry(entry, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {entry['name']!r} to {destination!r} failed.")
            raise

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            outcome = super().done(job)
            # An empty directory: made at the destination, it can go.
            if job.path and self.finished(job.path):
                outcome += self.remove(job.path)
            return outcome

        if job.kind == "rename":
            self.roots_renamed = (self.roots_renamed or 0) + 1
            return [(job.path, job.destination)]

        if job.kind == "rmdir":
            self.directories_moved += 1
        else:
            self.files_done += 1

        outcome: list[Any] = [(job.path, job.destination)]
        if (directory := self.child_done(job.path)) is not None:
            outcome += self.remove(directory)

        return outcome

    def remove(self, directory: str) -> list[Any]:
        """Delete a source directory now empty, and return what that moved at once.

        An implicit directory went with its last file: it is moved, with no
        request, as one found already gone is.
        """
        job = _Job("rmdir", {"name": directory}, self.target(directory))
        if self.implicit_directories:
            return self.done(job)
        self.waiting.append(job)
        return []

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        if job.kind == "rename":
            # Declared to rename directories, the storage failed this one: its
            # cause, not one failure per file.
            error.add_note(f"{self.doing} {job.path!r} to {job.destination!r} failed.")
            return [error]
        if job.kind != "rmdir":
            return super().failed(job, error)

        if isinstance(error, FileNotFoundError):
            return self.done(job)  # Already gone, as _tree_rm counts it.
        error.add_note(f"Deleting the directory {job.path!r} failed.")
        return [error]
