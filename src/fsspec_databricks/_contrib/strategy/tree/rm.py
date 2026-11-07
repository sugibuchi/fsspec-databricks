# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive delete that walks the tree and deletes while it walks.

`tree_rm` is an alternative to fsspec's recursive ``_rm``. fsspec's lists the
whole tree first, then deletes each path. `tree_rm` deletes each folder's files
as soon as the folder is listed, and each folder once it is empty.

This module depends on `_jobs`, and `jobs` and `operation` of `common`.
"""

from __future__ import annotations

import asyncio
from glob import has_magic
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies_common.states import RmState

from ..common.jobs import _Job, _Roots
from ..common.operation import Operation
from ._jobs import _expand, _TreeJobs

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Sequence

    from fsspec_alt_strategies_common.profile import StorageProfile

    from ..common.types import _Info, _Ls, _Request
    from .walk import _Listing

__all__ = ["RmState", "tree_rm"]


def tree_rm(
    path: str | Sequence[str],
    *,
    ls: _Ls,
    info: _Info,
    rm_file: _Request,
    rm_folder: _Request,
    rm_tree: _Request | None = None,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[str, RmState]:
    """Delete a folder and everything in it, deleting while the tree is walked.

    The tree is walked as `tree_walk` walks it. Each folder's files are deleted
    as soon as the folder is listed, while other folders are still being
    listed. A folder is deleted once everything in it is, so the deepest go
    first. A folder with anything in it that failed is kept.

    It yields each file and folder once deleted, and the error of each folder
    that failed to list or path that failed to be deleted. A path found already
    gone is not yielded.

    It wins over fsspec's ``_rm`` on storages that list one folder per request
    and have no recursive delete of their own, such as Databricks volumes. The
    deletions then start after the first listing, not after the last. It also
    keeps going past a failure, and reports each.

    It loses on object stores, which list a whole tree in one paginated request:
    fsspec's ``_rm`` makes fewer requests there. On a storage deleting a folder
    with everything in it in one request, that request wins: give it as
    ``rm_tree``.

    With ``rm_tree``, each folder root is first deleted whole by one
    ``rm_tree``, with no walk. A root whose deletion fails, e.g. as the
    storage refuses so large a folder, is walked and deleted file by file, as
    without it. A real failure then shows on its files. A root deleted whole
    is yielded once: what was in it is neither listed, yielded nor counted.

    A glob pattern deletes each path it matches, and all in it, as fsspec's
    ``_rm`` with ``recursive=True`` does. So does a list of paths and
    patterns. They are matched as `tree_glob` matches them, in one search,
    then the folders matched are walked at once.

    Once cancelled, it starts no new listing or deletion, and yields the
    outcomes of the deletions already sent. Closed, it waits for those
    deletions without yielding them, as their outcome would otherwise be
    unknown. Iterating it raises FileNotFoundError if ``path`` does not exist,
    or matches nothing.

    Requests: one ``ls`` per folder, one ``rm_file`` per file, and one
    ``rm_folder`` per folder. Patterns, or a list, add the requests of their
    `tree_glob`. With ``rm_tree``, a root deleted whole costs one ``rm_tree``
    instead. A single ``path`` then costs an ``info`` first, as ``rm_tree``
    is given folders only. Each request holds a permit of
    ``concurrency_limit`` while it runs.

    The profile's ``implicit_folders=True`` saves the ``rm_folder`` of each
    folder, as a folder goes with its last file. Such a folder is not yielded,
    as one found already gone is not.

    Parameters
    ----------
    path : str or sequence of str
        The folder to delete, as ``ls`` takes it. If it is a file, that file is
        deleted. If it is a glob pattern, or a list of paths and patterns, all
        they match is deleted.
    ls : callable
        Lists a folder, called as ``ls(path, detail=True)``: e.g. the
        filesystem's ``_ls``.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``. Only patterns use it.
    rm_file : callable
        Deletes a file, called as ``rm_file(path)``: e.g. the filesystem's
        ``_rm_file``.
    rm_folder : callable
        Deletes an empty folder, called as ``rm_folder(path)``: e.g. the
        filesystem's ``_rmdir``, or its ``_rm_file`` as fsspec's ``_rm`` does
        without one. A FileNotFoundError from it counts as deleted, as object
        stores have no folders to delete.
    rm_tree : callable, optional
        Deletes a folder with everything in it, in one request, called as
        ``rm_tree(path)`` on folders only: e.g. a storage's recursive delete.
        Without it, every folder is walked.
    profile : StorageProfile
        What the storage does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs. Share one to limit several
        operations, or files, together.

    Returns
    -------
    Operation
        Yielding each path deleted, or an error. Its state is an `RmState`.

    See Also
    --------
    tree_walk : The walk under it.

    Examples
    --------
    >>> rm = tree_rm(
    ...     "data/2024",
    ...     ls=fs._ls,
    ...     info=fs._info,
    ...     rm_file=fs._rm_file,
    ...     rm_folder=fs._rmdir,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with rm as op:
    ...     async for path in op:
    ...         if isinstance(path, Exception):
    ...             op.cancel()  # Stop at the first failure.
    ...             print(path)
    >>> op.state.files_deleted
    1234
    """
    return Operation(
        lambda op: _Deletion(
            ls,
            info,
            rm_file,
            rm_folder,
            rm_tree,
            path,
            profile,
            concurrency_limit,
            op,
        ).outcomes(),
        RmState(folders_deleted=0),
    )


class _Deletion(_TreeJobs):
    """The jobs of one `tree_rm`: a deletion per file, then per folder."""

    def __init__(
        self,
        ls: _Ls,
        info: _Info,
        rm_file: _Request,
        rm_folder: _Request,
        rm_tree: _Request | None,
        path: str | Sequence[str],
        profile: StorageProfile,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[str, RmState],
    ) -> None:
        super().__init__(ls, info, concurrency_limit, op)
        self.path = path
        self.rm_file = rm_file
        self.rm_folder = rm_folder
        self.rm_tree = rm_tree
        # The roots deleted whole, yielded with the first step.
        self.whole: list[str] = []
        self.implicit_folders = bool(profile.implicit_folders)
        self.files_found = 0
        self.files_deleted = 0
        self.folders_deleted = 0

    async def prepare(self) -> _Roots:
        """Find the roots, then delete whole the folders that can be: the rest are walked."""
        if not isinstance(self.path, str) or has_magic(self.path):
            roots = await _expand(self.ls, self.info, self.path, self.concurrency_limit)
            known = True  # The glob found the folders to be folders.
        else:
            roots = _Roots([self.path])
            known = False  # Of unknown kind, until asked.
        if self.rm_tree is None or self.op.cancelling():
            return roots
        sources = list(roots.folders)
        kinds = await asyncio.gather(*(self.delete_whole(s, known) for s in sources))
        self.whole = [s for s, k in zip(sources, kinds, strict=True) if k == "whole"]
        self.folders_deleted = len(self.whole)
        files = [s for s, k in zip(sources, kinds, strict=True) if k == "file"]
        return _Roots(
            [s for s, k in zip(sources, kinds, strict=True) if k == "walk"],
            [*roots.files, *files],
            roots.errors,
        )

    async def delete_whole(self, folder: str, known: bool) -> str:
        """Delete a root by one ``rm_tree``, and tell how it went: whole, file or walk.

        A root of unknown kind is asked first, as ``rm_tree`` takes folders
        only. A missing root, or a failed deletion, is walked instead: the walk
        then raises, or shows the failure on the files.
        """
        try:
            if not known:
                async with self.concurrency_limit:
                    if (await self.info(folder))["type"] != "directory":
                        return "file"
            async with self.concurrency_limit:
                await self.rm_tree(folder)  # type: ignore[misc]
        except Exception:
            return "walk"
        return "whole"

    async def begin(
        self, roots: _Roots, source: AsyncGenerator[_Listing] | None
    ) -> list[Any]:
        whole, self.whole = self.whole, []
        return [*whole, *await super().begin(roots, source)]

    def state(self) -> RmState:
        return RmState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_deleted=self.files_deleted,
            folders_deleted=self.folders_deleted,
        )

    def file_root(self, path: str) -> None:
        self.files_found = 1
        self.waiting.append(_Job("file", path))

    def add(self, listing: _Listing) -> None:
        self.track(listing)
        for entry in listing.entries:
            if entry["type"] != "directory":
                self.files_found += 1
                self.waiting.append(_Job("file", entry["name"].rstrip("/")))
        if not listing.entries:
            self.remove(listing.folder)

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "folder":
            return self.rm_folder(job.source)
        return self.rm_file(job.source)

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "folder":
            self.folders_deleted += 1
        else:
            self.files_deleted += 1
        self.after(job.source)
        return [job.source]

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        if isinstance(error, FileNotFoundError):
            # Already gone: an object store's folder, or deleted by another.
            self.after(job.source)
            return []
        # Yielded among paths, the error must tell its path itself.
        error.add_note(f"Deleting {job.source!r} failed.")
        return [error]

    def after(self, path: str) -> None:
        """Queue the deletion of a path's folder, if it is now empty."""
        if (folder := self.child_done(path)) is not None:
            self.remove(folder)

    def remove(self, folder: str) -> None:
        """Queue the deletion of an empty folder, unless it went with its last file."""
        if self.implicit_folders:
            self.after(folder)  # Gone already, as a FileNotFoundError tells.
        else:
            self.waiting.append(_Job("folder", folder))
