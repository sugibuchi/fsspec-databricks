# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The engine under the flat strategies that change a storage, such as flat_rm.

They list the keys under each root, and start their jobs, e.g. a batch
deletion, as the keys arrive: `_FlatJobs` feeds `common`'s `_Jobs` with the
keys. A subclass says what a key queues, how a job runs, and what its outcome
is. The roots are listed one after another.

A glob pattern is expanded first, with `flat_glob`, as fsspec's
``_expand_path`` expands it: the keys under its matching folders are then
listed.

This module depends on `_keys`, `find`, `glob`, and `jobs`, `operation` and
`paths` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from glob import has_magic
from typing import TYPE_CHECKING, Any

from ..common.jobs import _Jobs, _roots_of
from ..common.operation import _collect_with_errors
from ..common.paths import _trim
from ._keys import _close
from .find import _within
from .glob import flat_glob

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, Sequence

    from ..common.jobs import _Roots
    from ..common.operation import Operation
    from ..common.types import _IExpand, _Info


async def _expand(
    iexpand: _IExpand,
    info: _Info,
    pattern: str | Sequence[str],
    concurrency_limit: asyncio.Semaphore,
    maxdepth: int | None = None,
) -> _Roots:
    """The roots matching glob patterns, found with `flat_glob`."""
    matches, errors = await _collect_with_errors(
        flat_glob(
            pattern,
            iexpand=iexpand,
            info=info,
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
        )
    )
    return _roots_of(matches, errors, pattern)


def _keys_of(paths: tuple[str, ...]) -> str:
    """A batch's keys, for its error's note: all of a few, or the first and last."""
    if len(paths) <= 3:
        return ", ".join(map(repr, paths))
    return f"{len(paths)} keys, from {paths[0]!r} to {paths[-1]!r}"


class _FlatJobs(_Jobs):
    """The jobs of one operation, run while the keys under its roots are listed.

    With ``maxdepth``, the keys deeper than it are listed, but left out, as
    fsspec's ``_expand_path`` finds paths. A prefix lists every key under it.
    """

    def __init__(
        self,
        iexpand: _IExpand,
        info: _Info,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[Any, Any],
        maxdepth: int | None = None,
    ) -> None:
        super().__init__(concurrency_limit, op)

        self.iexpand = iexpand
        self.info = info
        self.maxdepth = maxdepth

        # The folder roots a pattern matched: fsspec takes them one level less
        # deep than a path named, as its glob counted that level already.
        self.matched: set[str] = set()

    async def glob_roots(self, pattern: str | Sequence[str]) -> _Roots:
        """The roots matching glob patterns, noting those a pattern matched."""
        roots = await _expand(
            self.iexpand, self.info, pattern, self.concurrency_limit, self.maxdepth
        )
        if self.maxdepth is not None:
            named = [pattern] if isinstance(pattern, str) else pattern
            literal = {_trim(p) for p in named if not has_magic(p)}
            self.matched = {f for f in roots.folders if f not in literal}
        return roots

    @abstractmethod
    def add(self, entry: dict[str, Any]) -> None:
        """Queue the jobs for a key: a file, or a folder's marker ending with "/"."""

    def file_root(self, path: str) -> None:
        self.add({"name": path, "type": "file"})

    def source(self, roots: _Roots) -> AsyncGenerator[dict[str, Any]] | None:
        """The keys under the root folders, if any, listed one after another."""
        if not roots.folders:
            return None
        return self._keys(roots.folders)

    def fed(self, item: dict[str, Any]) -> list[Any]:
        self.current = item["name"]
        self.add(item)
        return []

    async def _keys(self, folders: Sequence[str]) -> AsyncGenerator[dict[str, Any]]:
        """The keys under each folder, as ``iexpand`` lists them.

        A folder with nothing under it is asked for with ``info``: a file is
        yielded as a key, and a missing one raises FileNotFoundError, ending the
        operation, as a missing root does.
        """
        for folder in folders:
            depth = self.maxdepth
            if depth is not None and folder in self.matched:
                depth -= 1
                if depth < 1:
                    continue  # At the limit already: nothing in it is taken.

            under = False
            prefix = f"{folder}/" if folder else ""
            entries = self.iexpand(prefix)
            try:
                async for entry in entries:
                    under = True
                    if _within(entry["name"].rstrip("/")[len(prefix) :], depth):
                        yield entry
                    if self.op.cancelling():
                        return
            finally:
                await _close(entries)

            if under or not folder:
                continue

            async with self.concurrency_limit:
                entry = await self.info(folder)
            if entry["type"] != "directory":
                yield entry
