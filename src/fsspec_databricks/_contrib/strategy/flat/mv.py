# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive move of the keys of a prefix, each moved as soon as it is listed.

`flat_mv` is an alternative to fsspec's recursive ``mv`` for flat storages.
fsspec's copies every key, then deletes them all, each stage listing the
prefix first. `flat_mv` moves each key as soon as the listing gives it: by one
``mv_file``, or by a copy, deleting the copied keys in batches while the
listing goes on.

This module depends on `_jobs`, `cp`, and `jobs` and `operation` of `common`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import MvState

from ..common.jobs import _Job
from ..common.operation import Operation
from ._jobs import _keys_of
from .cp import _Copy, _CopyOrMove

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Sequence

    from fsspec_alt_strategies.profile import StorageProfile

    from ..common.types import _IExpand, _Info, _Request, _Requests

__all__ = ["MvState", "flat_local_mv", "flat_mv"]


def flat_mv(
    path1: str | Sequence[str],
    path2: str,
    *,
    iexpand: _IExpand,
    info: _Info,
    mv_file: _Request | None = None,
    cp_file: _Request | None = None,
    rm_files: _Requests | None = None,
    batch_size: int | None = None,
    dst_info: _Info,
    dst_makedirs: _Request,
    dst_rm_file: _Request,
    same_storage: bool,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], MvState]:
    """Move the keys under a prefix, each as soon as it is listed.

    It places the paths as fsspec's ``mv`` with ``recursive=True`` does, as
    `flat_cp` places them. Existing files are overwritten.

    It moves each key in one of two ways, by the operations given, and by the
    profile if both are:

    - ``mv_file``: each key is moved by one ``mv_file`` as soon as the
      listing gives it, and yielded once moved. It wins on storages renaming a
      key in one request, which copies no data.
    - ``cp_file``, ``rm_files`` and ``batch_size``: each key is copied by one
      ``cp_file`` as soon as the listing gives it. The keys copied are deleted
      in batches of ``batch_size``, each by one ``rm_files``, and the last,
      smaller batch once all is copied. A key is moved, and yielded, once its
      batch is deleted. It wins on object stores, which rename nothing, but
      delete many keys in one request.

    Given both, it moves by ``mv_file`` only within one storage whose
    profile's ``renames_files`` is True. Unknown, it copies and deletes in
    batches, which is right on any storage: about one request per key either
    way, but a rename copies no data.

    The destination is described by the ``dst_`` operations and ``profile``,
    as with `flat_cp`. Between two storages, ``same_storage`` is False, and
    ``cp_file`` copies to the other storage. ``mv_file`` then copies, and
    deletes the source key. ``rm_files`` always deletes the source's.

    A folder's marker, a key ending with "/", is moved too, but not yielded. In
    batches, it is deleted without being copied, as `flat_cp` copies none: its
    folder is made at the destination instead, as `flat_cp` makes it.

    Unlike fsspec's ``mv``, it keeps going past a failure, and reports each. A
    key that failed to move, or to copy, stays where it was. If a batch fails
    to be deleted, its keys are then in both places, as its error notes.

    It wins over fsspec's ``mv`` on large prefixes of flat storages: the moves
    start after the first page of the listing, and the prefix is listed once,
    not twice.

    Once cancelled, it takes nothing more from the listing, and starts no new
    move or copy. In batches, it still deletes the keys already copied, so
    that none is in both places. It yields the outcomes of what was sent.
    Closed, it waits for the requests in flight without yielding them: keys
    copied but not yet deleted are then in both places. Iterating it raises
    FileNotFoundError if ``path1`` does not exist, or matches nothing, and
    ValueError if ``path2`` is inside it.

    Requests: as `flat_cp`, with one ``mv_file`` per key instead of its
    ``cp_file``, or with one ``rm_files`` more per batch.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to move, a glob pattern, or a list of them, as
        ``iexpand`` takes paths.
    path2 : str
        Where to move it, as with `flat_cp`.
    iexpand, info : callable
        The storage's operations, as with `flat_cp`: ``iexpand`` lists each key
        once.
    mv_file : callable, optional
        Moves a file, called as ``mv_file(source, destination)``: e.g. the
        filesystem's ``_mv_file``.
    cp_file : callable, optional
        Copies a file, as with `flat_cp`.
    rm_files : callable, optional
        Deletes several source files in one request, as with `flat_rm`.
    batch_size : int, optional
        The most keys of one ``rm_files``, e.g. 1000 on S3. ``cp_file``,
        ``rm_files`` and ``batch_size`` are given together, or not at all.
    dst_info, dst_makedirs, dst_rm_file, same_storage : callable or bool
        The destination's, as with `flat_cp`.
    profile : StorageProfile
        What the destination does: ``StorageProfile()`` if it is unknown. Its
        ``renames_files`` chooses the way when both are given.
    concurrency_limit : asyncio.Semaphore
        Held by each request but the listing while it runs. Share one to
        limit several operations, or files, together.

    Returns
    -------
    Operation
        Yielding ``(source, destination)`` for each key moved, or an error. Its
        state is an `MvState`.

    Raises
    ------
    ValueError
        If neither ``mv_file`` nor all of ``cp_file``, ``rm_files`` and
        ``batch_size`` are given, only some of those three, or
        ``batch_size`` is less than 1.

    See Also
    --------
    flat_cp : The same, keeping the sources.
    tree_mv : The same, for storages listing one folder per request.

    Examples
    --------
    >>> mv = flat_mv(
    ...     "data/2024",
    ...     "archive/",
    ...     iexpand=my_ils,
    ...     info=fs._info,
    ...     cp_file=fs._cp_file,
    ...     rm_files=my_rm_files,
    ...     batch_size=1000,
    ...     dst_info=fs._info,
    ...     dst_makedirs=fs._makedirs,
    ...     dst_rm_file=fs._rm_file,
    ...     same_storage=True,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async for moved in mv:
    ...     if isinstance(moved, Exception):
    ...         print("failed:", moved)
    """
    batched = [o is not None for o in (cp_file, rm_files, batch_size)]
    if any(batched) and not all(batched):
        raise ValueError("cp_file, rm_files and batch_size are given together")
    if mv_file is None and not all(batched):
        raise ValueError("Give mv_file, or all of cp_file, rm_files and batch_size")
    if batch_size is not None and batch_size < 1:
        raise ValueError(f"batch_size must be at least 1: {batch_size}")

    # Both given, a rename wins only where the storage is known to rename.
    renames = same_storage and profile.renames_files is True
    if all(batched) and not renames:
        mv_file = None
    elif mv_file is not None:
        cp_file = rm_files = batch_size = None

    destination = {
        "destination_info": dst_info,
        "makedirs": dst_makedirs,
        "rm_file": dst_rm_file,
        "same_storage": same_storage,
        "profile": profile,
    }

    if mv_file is not None:
        return Operation(
            lambda op: _Rename(
                path1,
                path2,
                iexpand=iexpand,
                info=info,
                mv_file=mv_file,
                concurrency_limit=concurrency_limit,
                op=op,
                **destination,
            ).outcomes(),
            MvState(),
        )

    return Operation(
        lambda op: _Move(
            path1,
            path2,
            iexpand=iexpand,
            info=info,
            cp_file=cp_file,  # type: ignore[arg-type]
            rm_files=rm_files,  # type: ignore[arg-type]
            batch_size=batch_size,  # type: ignore[arg-type]
            concurrency_limit=concurrency_limit,
            op=op,
            **destination,
        ).outcomes(),
        MvState(),
    )


def flat_local_mv(
    path1: str | Sequence[str],
    path2: str,
    *,
    iexpand: _IExpand,
    info: _Info,
    mv_file: _Request | None = None,
    cp_file: _Request | None = None,
    rm_files: _Requests | None = None,
    batch_size: int | None = None,
    makedirs: _Request,
    rm_file: _Request,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], MvState]:
    """Move the keys under a prefix within one storage, as `flat_mv` does.

    It is `flat_mv` with the destination's operations the source's own, and
    ``same_storage=True``: the profile's ``renames_files`` then chooses a
    rename, and a path moved under itself is refused.

    Parameters
    ----------
    path1 : str or sequence of str
        The folder or file to move, a glob pattern, or a list of them.
    path2 : str
        Where to move it, as with `flat_mv`.
    iexpand, info, mv_file, cp_file, rm_files, batch_size : callable or int
        The storage's operations, as with `flat_mv`.
    makedirs, rm_file : callable
        The storage's, as `flat_mv`'s ``dst_makedirs`` and ``dst_rm_file``.
    profile : StorageProfile
        What the storage does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request but the listing while it runs.

    Returns
    -------
    Operation
        As `flat_mv`'s.

    Raises
    ------
    ValueError
        As `flat_mv` raises it.

    See Also
    --------
    flat_mv : The same, to another storage too.
    """
    return flat_mv(
        path1,
        path2,
        iexpand=iexpand,
        info=info,
        mv_file=mv_file,
        cp_file=cp_file,
        rm_files=rm_files,
        batch_size=batch_size,
        dst_info=info,
        dst_makedirs=makedirs,
        dst_rm_file=rm_file,
        same_storage=True,
        profile=profile,
        concurrency_limit=concurrency_limit,
    )


class _Rename(_CopyOrMove):
    """The jobs of one `flat_mv` by ``mv_file``: one move per key.

    ``mv_file`` moves each key where a copy would put it. A folder's marker is
    moved too, but not yielded.
    """

    verb = "move"
    doing = "Moving"

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        mv_file: _Request,
        rm_file: _Request,
        **kwargs: Any,
    ) -> None:
        # rm_file is the destination's, which a move never needs.
        super().__init__(path1, path2, **kwargs)
        self.mv_file = mv_file

    async def transfer_file(self, source: str, destination: str) -> None:
        try:
            await self.mv_file(source, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise

    def state(self) -> MvState:
        return MvState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_moved=self.files_done,
        )

    def add(self, entry: dict[str, Any]) -> None:
        name = entry["name"]
        if name.endswith("/"):
            self.waiting.append(_Job("marker", name, self.target(name)))
            return
        super().add(entry)

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "marker":
            return []
        return super().done(job)


class _Move(_Copy):
    """The jobs of one `flat_mv` by copies: a copy per key, then a deletion per batch."""

    verb = "move"
    doing = "Moving"

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        rm_files: _Requests,
        batch_size: int,
        **kwargs: Any,
    ) -> None:
        super().__init__(path1, path2, **kwargs)

        self.rm_files = rm_files
        self.batch_size = batch_size

        # The keys to delete. Each key moving, from its listing to its
        # deletion, has where it is copied, None for a marker: listed again,
        # it is skipped.
        self.batch: list[str] = []
        self.copied: dict[str, str | None] = {}

        # The copies queued or running, and whether the listing has ended: the
        # last batch goes once both say all is copied.
        self.copying = 0
        self.listed = False

    def state(self) -> MvState:
        return MvState(
            errors=self.errors,
            current=self.current,
            files_found=self.files_found,
            files_moved=self.files_done,
        )

    def add(self, entry: dict[str, Any]) -> None:
        name = entry["name"]
        if name in self.copied:
            return
        if name.endswith("/"):
            # A folder's marker: deleted, its folder made as flat_cp makes it.
            self.copied[name] = None
            self.delete_later(name)
            super().add(entry)
            return

        self.copied[name] = self.target(name)
        self.copying += 1
        super().add(entry)

    def exhausted(self) -> None:
        self.listed = True
        self.send_if_ready()

    def stopping(self) -> None:
        # The copies not started are dropped, but the keys copied are deleted,
        # so that none is in both places.
        deletions = [j for j in self.waiting if j.kind == "delete"]
        self.waiting.clear()
        self.waiting.extend(deletions)
        self.copying = sum(j.kind == "copy" for j in self.running.values())
        self.send_if_ready()

    def run(self, job: _Job) -> Awaitable[Any]:
        if job.kind == "delete":
            return self.rm_files(list(job.paths))
        return super().run(job)

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            return super().done(job)
        if job.kind == "copy":
            self.copying -= 1
            self.delete_later(job.source)
            return []

        moved = []
        for path in job.paths:
            if (destination := self.copied.pop(path)) is not None:
                self.files_done += 1
                moved.append((path, destination))
        return moved

    def failed(self, job: _Job, error: Exception) -> list[Any]:
        if job.kind == "mkdir":
            return super().failed(job, error)
        if job.kind == "copy":
            self.copying -= 1
            self.copied.pop(job.source)
            self.send_if_ready()
            return super().failed(job, error)

        for path in job.paths:
            self.copied.pop(path)

        error.add_note(
            f"Deleting {_keys_of(job.paths)} failed, once copied: they are in both"
            " places."
        )
        return [error]

    def delete_later(self, source: str) -> None:
        """Add a key to the batch to delete, and send the batch if it is ready."""
        self.batch.append(source)
        self.send_if_ready()

    def send_if_ready(self) -> None:
        """Send the batch if full, or if nothing more can join it."""
        if not self.batch:
            return

        done = (self.listed or self.op.cancelling()) and self.copying == 0
        if len(self.batch) >= self.batch_size or done:
            paths, self.batch = tuple(self.batch), []
            self.waiting.append(_Job("delete", paths[-1], paths=paths))
