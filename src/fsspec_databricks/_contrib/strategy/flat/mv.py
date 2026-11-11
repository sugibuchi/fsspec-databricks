# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive move of the keys of a prefix, each moved as soon as it is listed.

`_flat_mv` is an alternative to fsspec's recursive ``mv`` for flat storages.
fsspec's copies every key, then deletes them all, each stage listing the
prefix first. `_flat_mv` moves each key as soon as the listing gives it: by
one ``_mv_file_entry``, or by a copy, deleting the copied keys in batches while the
listing goes on.

This module depends on `_jobs`, `cp`, and `jobs`, `operation` and `paths` of
`common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import MvState

from ..common.jobs import _Job, _transfer_of
from ..common.operation import Operation
from ..common.paths import _check_maxdepth
from ._jobs import _check_batch_size, _keys_of
from .cp import _Copy, _CopyOrMove

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Sequence

    from ..common.jobs import _Transfer

__all__: list[str] = []


def _flat_mv(
    src_fs: Any,
    dst_fs: Any,
    path1: str | Sequence[str],
    path2: str,
    *,
    cp_entry: _Transfer | None = None,
    same_storage: bool | None = None,
    maxdepth: int | None = None,
) -> Operation[tuple[str, str], MvState]:
    """Move the keys under a prefix, each as soon as it is listed.

    It places the paths as fsspec's ``mv`` with ``recursive=True`` does, as
    `_flat_cp` places them. Existing files are overwritten.

    It moves each key in one of two ways, by the destination's profile:

    - by the source's ``_mv_file_entry``: each key is moved by one ``_mv_file_entry`` as
      soon as the listing gives it, and yielded once moved. It wins on
      storages renaming a key in one request, which copies no data.
    - by copies: each key is copied by one ``cp_entry`` as soon as the listing
      gives it. The keys copied are deleted in batches of the source's
      ``rm_batch_size``, each by one of its ``_rm_files``, and the last,
      smaller batch once all is copied. A key is moved, and yielded, once its
      batch is deleted. It wins on object stores, which rename nothing, but
      delete many keys in one request.

    It moves by ``_mv_file_entry`` only within one storage whose profile's
    ``renames_files`` is True. Unknown, it copies and deletes in batches,
    which is right on any storage: about one request per key either way, but
    a rename copies no data.

    The destination is described by ``dst_fs`` and its profile, as with
    `_flat_cp`. Between two storages, ``same_storage`` is False, and
    ``cp_entry`` copies to the other storage. ``_rm_files`` always deletes the
    source's.

    A directory's marker, a key ending with "/", is moved too, but not yielded. In
    batches, it is deleted without being copied, as `_flat_cp` copies none: its
    directory is made at the destination instead, as `_flat_cp` makes it.

    With ``maxdepth``, it moves the keys `_flat_cp` copies with it, and no
    marker. fsspec's ``mv`` copies as its ``copy`` does with ``maxdepth``, but
    then deletes the whole source prefix, deeper keys too. This keeps them.

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

    Requests: as `_flat_cp`, with one ``_mv_file_entry`` per key instead of its
    ``cp_entry``, or with one ``_rm_files`` more per batch.

    ``src_fs`` is the filesystem moved from, and ``dst_fs`` the one moved to,
    ``src_fs`` again within one storage. ``path1`` is a directory, a file, a glob
    pattern, or a list of them, as the source's ``_ils`` takes paths.
    ``path2`` places them as `_flat_cp` does. ``cp_entry(entry,
    destination)`` copies a file by its source entry when the move copies: the source's
    ``_cp_file_entry`` by default, within one storage. ``same_storage`` is whether
    ``dst_fs`` is ``src_fs``, unless given. ``maxdepth`` of 1 moves only the
    keys right in ``path1``.

    The operation yields ``(source, destination)`` for each key moved, or an
    error, and its state is an `MvState`. The call raises ValueError if the
    source's ``rm_batch_size`` or ``maxdepth`` is less than 1, or ``cp_entry``
    is missing between two storages.
    """
    _check_maxdepth(maxdepth)
    if same_storage is None:
        same_storage = src_fs is dst_fs
    options = {
        "fs": src_fs,
        "dst_fs": dst_fs,
        "path1": path1,
        "path2": path2,
        "same_storage": same_storage,
        "maxdepth": maxdepth,
    }

    _check_batch_size(src_fs.rm_batch_size)
    cp_entry, _ = _transfer_of(src_fs, dst_fs, cp_entry, same_storage, "cp_entry")

    async def run(op: Operation[Any, MvState]) -> AsyncGenerator[Any]:
        # A rename wins only where the storage is known to rename: the profile,
        # awaited, chooses the jobs.
        renames = (
            same_storage and (await dst_fs._get_storage_profile()).renames_files is True
        )
        jobs: _CopyOrMove = (
            _Rename(op=op, **options)
            if renames
            else _Move(op=op, cp_entry=cp_entry, **options)
        )
        async with aclosing(jobs.outcomes()) as outcomes:
            async for outcome in outcomes:
                yield outcome

    return Operation(run, MvState())


class _Rename(_CopyOrMove):
    """The jobs of one `_flat_mv` by ``_mv_file_entry``: one move per key.

    The source's ``_mv_file_entry`` moves each key where a copy would put it. A
    directory's marker is moved too, but not yielded.
    """

    verb = "move"
    doing = "Moving"

    async def setup(self) -> None:
        await super().setup()

        # A rename makes no parent, as a write may: parents_required tells of
        # writes only, and _mv_file_entry is called only where the parent exists.
        self.parents_required = not self.implicit_directories

    async def transfer_file(self, entry: dict[str, Any], destination: str) -> None:
        try:
            await self.fs._mv_file_entry(entry, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {entry['name']!r} to {destination!r} failed.")
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
            # With maxdepth, directories stay, as fsspec's copy leaves them out.
            if self.maxdepth is None:
                job = _Job("marker", entry, self.target(name))
                self.waiting.append(job)
            return
        super().add(entry)

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "marker":
            return []
        return super().done(job)


class _Move(_Copy):
    """The jobs of one `_flat_mv` by copies: a copy per key, then a deletion per batch."""

    verb = "move"
    doing = "Moving"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self.batch_size: int = self.fs.rm_batch_size

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
            if self.maxdepth is not None:
                return  # Directories stay, as fsspec's copy leaves them out.
            # A directory's marker: deleted, its directory made as _flat_cp makes it.
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
            return self.fs._rm_files(list(job.paths))
        return super().run(job)

    def done(self, job: _Job) -> list[Any]:
        if job.kind == "mkdir":
            return super().done(job)
        if job.kind == "copy":
            self.copying -= 1
            self.delete_later(job.path)
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
            self.copied.pop(job.path)
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
            self.waiting.append(_Job("delete", {"name": paths[-1]}, paths=paths))
