# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive delete that deletes the keys of a prefix in batches, as it lists them.

`flat_rm` is an alternative to fsspec's recursive ``_rm`` for flat storages.
fsspec's, and the object stores' own, list every key first, then delete them,
in batches where the storage can. `flat_rm` sends each batch as soon as the
listing has given its keys.

This module depends on `_jobs`, and `jobs`, `operation` and `paths` of `common`.
"""

from __future__ import annotations

from glob import has_magic
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import RmState

from ..common.jobs import _Job, _Roots
from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from ._jobs import _FlatJobs, _keys_of

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Sequence

    from ..common.types import _IExpand, _Info, _Requests

__all__ = ["RmState", "flat_rm"]


def flat_rm(
    path: str | Sequence[str],
    *,
    iexpand: _IExpand,
    info: _Info,
    rm_files: _Requests,
    batch_size: int,
    maxdepth: int | None = None,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[str, RmState]:
    """Delete the keys under a prefix, in batches sent as the listing goes on.

    It deletes what fsspec's ``_rm`` with ``recursive=True`` deletes: the
    keys under ``path``, or ``path`` itself if it is a file. A folder's
    marker, a key ending with "/", is deleted as any key is. A glob pattern, or
    a list of paths and patterns, deletes all they match, as with fsspec's
    ``_rm``. They are matched as `flat_glob` matches them, then the keys under
    the folders matched are listed, one folder after another.

    With ``maxdepth``, it deletes the keys fsspec's ``_rm`` deletes with it:
    those at most ``maxdepth`` levels below ``path``, or below a pattern's
    matches one level less. A folder's marker counts at its folder's level.
    The keys deeper are listed all the same, as a prefix lists every key
    under it, but kept.

    The keys are grouped in batches of ``batch_size``, each deleted by one
    ``rm_files``, while the listing goes on. The last batch, smaller, goes once
    the listing ends. It yields each key once its batch is deleted, and the
    error of each batch that failed, noting its keys.

    It wins over fsspec's ``_rm`` on large prefixes of flat storages: the
    deletions start after the first page of the listing, not after the last,
    and the keys are held a batch at a time. It sends the same requests as a
    storage deleting in batches of ``batch_size``. It loses on storages with a
    hierarchical namespace, whose folders a listing by prefix does not see:
    `tree_rm` suits those.

    Once cancelled, it takes nothing more from the listing, and sends no new
    batch, but yields the outcomes of those sent. Closed, it waits for those
    without yielding them. Iterating it raises FileNotFoundError if ``path``
    does not exist, or matches nothing. An error from the listing ends it too.

    Requests: the pages of one ``iexpand`` per folder, one after another, and one
    ``rm_files`` per batch, as ``concurrency_limit`` allows. One ``info`` for
    ``path`` if nothing is under it. Patterns, or a list, add the requests of
    their `flat_glob`.

    Parameters
    ----------
    path : str or sequence of str
        The folder to delete, as ``iexpand`` takes paths. If it is a file, that
        file is deleted. If it is a glob pattern, or a list of paths and
        patterns, all they match is deleted.
    iexpand : callable
        Lists every key starting with a prefix, called as ``iexpand(prefix)``: an
        async iterator of entries as fsspec's ``_ls`` gives them, in order of
        their names, each key once, as an object store lists them.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    rm_files : callable
        Deletes several files in one request, called as ``rm_files(paths)``
        with a list: e.g. wrapping S3's DeleteObjects. A key missing is no
        error. It raises if any is not deleted, as the whole batch then fails.
    batch_size : int
        The most keys of one ``rm_files``, e.g. 1000 on S3.
    maxdepth : int, optional
        The deepest level to delete: 1 deletes only the keys right in
        ``path``. No limit by default.
    concurrency_limit : asyncio.Semaphore
        Held by each ``rm_files`` and ``info`` while it runs. The listing holds
        none. Share one to limit several operations, or files, together.

    Returns
    -------
    Operation
        Yielding each key deleted, or an error. Its state is an `RmState`.

    Raises
    ------
    ValueError
        If ``batch_size`` or ``maxdepth`` is less than 1.

    See Also
    --------
    tree_rm : The same, for storages listing one folder per request.

    Examples
    --------
    >>> rm = flat_rm(
    ...     "data/2024",
    ...     iexpand=my_ils,
    ...     info=fs._info,
    ...     rm_files=my_rm_files,
    ...     batch_size=1000,
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with rm as op:
    ...     async for path in op:
    ...         if isinstance(path, Exception):
    ...             print("failed:", path)
    >>> op.state.files_deleted
    1234
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be at least 1: {batch_size}")
    _check_maxdepth(maxdepth)

    return Operation(
        lambda op: _Deletion(
            path,
            iexpand=iexpand,
            info=info,
            rm_files=rm_files,
            batch_size=batch_size,
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
            op=op,
        ).outcomes(),
        RmState(),
    )


class _Deletion(_FlatJobs):
    """The jobs of one `flat_rm`: a deletion per batch of keys."""

    def __init__(
        self,
        path: str | Sequence[str],
        *,
        iexpand: _IExpand,
        info: _Info,
        rm_files: _Requests,
        batch_size: int,
        maxdepth: int | None,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[str, RmState],
    ) -> None:
        super().__init__(iexpand, info, concurrency_limit, op, maxdepth)

        self.path = path
        self.rm_files = rm_files
        self.batch_size = batch_size

        self.batch: list[str] = []
        self.files_found = 0
        self.files_deleted = 0

    async def prepare(self) -> _Roots:
        if not isinstance(self.path, str) or has_magic(self.path):
            return await self.glob_roots(self.path)
        return _Roots([_trim(self.path)])

    def state(self) -> RmState:
        return RmState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_deleted=self.files_deleted,
        )

    def add(self, entry: dict[str, Any]) -> None:
        self.files_found += 1
        self.batch.append(entry["name"])
        if len(self.batch) >= self.batch_size:
            self.send()

    def exhausted(self) -> None:
        if self.batch:
            self.send()

    def run(self, job: _Job) -> Awaitable[Any]:
        return self.rm_files(list(job.paths))

    def done(self, job: _Job) -> list[Any]:
        self.files_deleted += len(job.paths)
        return list(job.paths)

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        # Yielded among paths, the error must tell its paths itself.
        error.add_note(f"Deleting {_keys_of(job.paths)} failed.")
        return [error]

    def send(self) -> None:
        """Queue the deletion of the batch, and start another."""
        paths, self.batch = tuple(self.batch), []
        self.waiting.append(_Job("delete", paths[-1], paths=paths))
