# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The engine under the tree strategies that change a tree, such as tree_rm.

They walk the tree as tree_walk does, and start their jobs, e.g. deleting a
file, as each folder's listing arrives: `_TreeJobs` feeds `common`'s `_Jobs`
with the walk's listings. A subclass says what a listing queues, how a job
runs, and what its outcome is.

A glob pattern is expanded first, as fsspec's ``_expand_path`` expands it:
its matching folders are then walked at once.

This module depends on `glob`, `walk`, and `jobs` and `operation` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from ..common.jobs import _Jobs, _roots_of
from ..common.operation import _collect_with_errors
from .glob import tree_glob
from .walk import _listings

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, Sequence

    from ..common.jobs import _Roots
    from ..common.operation import Operation
    from ..common.types import _Info, _Ls
    from .walk import _Listing


async def _expand(
    ls: _Ls,
    info: _Info,
    pattern: str | Sequence[str],
    concurrency_limit: asyncio.Semaphore,
) -> _Roots:
    """The roots matching glob patterns, found with `tree_glob`."""
    matches, errors = await _collect_with_errors(
        tree_glob(pattern, ls=ls, info=info, concurrency_limit=concurrency_limit)
    )
    return _roots_of(matches, errors, pattern)


class _TreeJobs(_Jobs):
    """The jobs of one operation, run while its tree is walked."""

    def __init__(
        self,
        ls: _Ls,
        info: _Info,
        concurrency_limit: asyncio.Semaphore,
        op: Operation[Any, Any],
    ) -> None:
        super().__init__(concurrency_limit, op)

        self.ls = ls
        self.info = info

        # The paths in each folder listed that are not done yet, for strategies
        # finishing a folder after all in it, as a deletion does. A folder with
        # a failure in it never reaches zero, and nor do the folders above it.
        self.remaining: dict[str, int] = {}
        self.parents: dict[str, str] = {}

    @abstractmethod
    def add(self, listing: _Listing) -> None:
        """Queue the jobs for a folder listed without error."""

    def source(self, roots: _Roots) -> AsyncGenerator[_Listing] | None:
        """The listings of the root folders, if any, walked at once."""
        if not roots.folders:
            return None
        return _listings(
            self.ls,
            roots.folders,
            concurrency_limit=self.concurrency_limit,
            stopping=self.op.cancelling,
        )

    async def begin(
        self, roots: _Roots, source: AsyncGenerator[_Listing] | None
    ) -> list[Any]:
        """Queue the files' jobs, and list the folders: the first step.

        A single root of unknown kind is among the folders: if it is a file,
        its listing says so, and its jobs are queued instead.
        """
        try:
            return await super().begin(roots, source)
        except NotADirectoryError as e:
            self.file_root(e.args[0])
            return []

    def fed(self, item: _Listing) -> list[Any]:
        self.current = item.folder
        if item.error is not None:
            self.errors += 1
            return [item.error]
        self.add(item)
        return []

    def track(self, listing: _Listing) -> None:
        """Count the paths in a listed folder, for `child_done`.

        A storage may list a path twice, e.g. a folder as a prefix and as its
        marker object. Each path counts once, as the walk lists a folder once.
        """
        names = {entry["name"].rstrip("/") for entry in listing.entries}
        self.remaining[listing.folder] = len(names)
        self.parents.update(dict.fromkeys(names, listing.folder))

    def child_done(self, path: str) -> str | None:
        """Count a path done, and return its folder if all in it is now done.

        A path done twice, as one listed twice is, counts once.
        """
        parent = self.parents.pop(path, None)
        if parent is None:
            return None
        self.remaining[parent] -= 1
        return parent if self.remaining[parent] == 0 else None
