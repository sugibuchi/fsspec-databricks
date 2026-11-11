# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The engine under the flat strategies that change a storage, such as _flat_rm.

They list the keys under each root, and start their jobs, e.g. a batch
deletion, as the keys arrive: `_FlatJobs` feeds `common`'s `_Jobs` with the
keys. A subclass says what a key queues, how a job runs, and what its outcome
is. The roots are listed one after another.

A glob pattern is expanded first, with `_flat_glob`, as fsspec's
``_expand_path`` expands it: the keys under its matching directories are then
listed.

This module depends on `_keys`, `find`, `glob`, and `jobs` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from ..common.jobs import _Jobs
from ._keys import _close, _listing
from .find import _within
from .glob import _flat_glob

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from ..common.jobs import _Roots
    from ..common.operation import Operation


def _check_batch_size(batch_size: int) -> None:
    """Refuse a filesystem's ``rm_batch_size`` below 1, as no batch could be sent."""
    if batch_size < 1:
        raise ValueError(f"batch_size must be at least 1: {batch_size}")


def _keys_of(paths: tuple[str, ...]) -> str:
    """A batch's keys, for its error's note: all of a few, or the first and last."""
    if len(paths) <= 3:
        return ", ".join(map(repr, paths))
    return f"{len(paths)} keys, from {paths[0]!r} to {paths[-1]!r}"


class _FlatJobs(_Jobs):
    """The jobs of one operation, run while the keys under its roots are listed.

    ``fs`` is the filesystem listed, by its recursive ``_ils`` and ``_info``.
    With ``maxdepth``, the keys deeper than it are listed, as a prefix lists
    every key under it, but left out, as fsspec's ``_expand_path`` finds paths.

    A batch deletion runs by `delete`, and its outcome is told by `refused`:
    the keys ``_rm_files`` could not delete, each with its error.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # The keys each batch deleted could not delete, by the batch's keys.
        self._refused: dict[tuple[str, ...], dict[str, Exception]] = {}

    async def delete(self, paths: tuple[str, ...]) -> None:
        """Delete a batch of keys by ``_rm_files``, keeping those it refused for `refused`."""
        refused = await self.fs._rm_files(list(paths))
        if refused:
            self._refused[paths] = {k: e for k, e in refused.items() if k in paths}

    def refused(self, paths: tuple[str, ...], note: str) -> dict[str, Exception]:
        """The keys a batch deleted could not delete, each error noted and counted.

        ``note`` tells a key's failure, its ``{key}`` the key.
        """
        refused = self._refused.pop(paths, {})
        for key, error in refused.items():
            error.add_note(note.format(key=repr(key)))
        self.errors += len(refused)
        return refused

    def glob(self, pattern: str | Sequence[str]) -> Operation[Any, Any]:
        return _flat_glob(self.fs, pattern, maxdepth=self.maxdepth)

    @abstractmethod
    def add(self, entry: dict[str, Any]) -> None:
        """Queue the jobs for a key: a file, or a directory's marker ending with "/"."""

    def file_root(self, entry: dict[str, Any]) -> None:
        self.add(entry)

    def source(self, roots: _Roots) -> AsyncGenerator[dict[str, Any]] | None:
        """The keys under the root directories, if any, listed one after another."""
        if not roots.directories:
            return None
        return self._keys([d["name"] for d in roots.directories])

    def fed(self, item: dict[str, Any]) -> list[Any]:
        self.current = item["name"]
        self.add(item)
        return []

    async def _keys(self, directories: Sequence[str]) -> AsyncGenerator[dict[str, Any]]:
        """The keys under each directory, as its recursive ``_ils`` lists them.

        A directory with nothing under it is asked for with ``_info``: a file is
        yielded as a key, and a missing one raises FileNotFoundError, ending the
        operation, as a missing root does.
        """
        for directory in directories:
            depth = self.maxdepth
            if depth is not None and directory in self.matched:
                depth -= 1
                if depth < 1:
                    continue  # At the limit already: nothing in it is taken.

            under = False
            prefix = f"{directory}/" if directory else ""
            entries = _listing(self.fs, prefix)
            try:
                async for entry in entries:
                    under = True
                    if _within(entry["name"].rstrip("/")[len(prefix) :], depth):
                        yield entry
                    if self.op.cancelling():
                        return
            finally:
                await _close(entries)

            if under or not directory:
                continue

            async with self.concurrency_limit:
                entry = await self.fs._info(directory)
            if entry["type"] != "directory":
                yield entry
