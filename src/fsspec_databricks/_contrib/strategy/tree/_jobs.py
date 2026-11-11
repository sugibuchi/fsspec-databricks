# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The engine under the tree strategies that change a tree, such as _tree_rm.

They walk the tree as _tree_walk does, and start their jobs, e.g. deleting a
file, as each part of a directory's listing arrives. `_TreeJobs` feeds
`common`'s `_Jobs` with the parts of `_chunks`, so that a directory of many
files is never held whole. A subclass says what a listing queues, how a job
runs, and what its outcome is.

A glob pattern is expanded first, as fsspec's ``_expand_path`` expands it:
its matching directories are then walked at once.

This module depends on `glob`, `walk`, and `jobs` of `common`.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from ..common.jobs import _Jobs
from .glob import _tree_glob
from .walk import _chunks

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from ..common.jobs import _Roots
    from ..common.operation import Operation
    from .walk import _Listing


class _TreeJobs(_Jobs):
    """The jobs of one operation, run while the tree of ``fs`` is walked.

    ``fs`` is the filesystem walked, by its ``_ils`` and ``_info``, each
    request holding a permit of its ``operation_limit``. With ``maxdepth``, the
    walk lists no directory deeper than it, as fsspec's ``_expand_path`` finds
    paths. Each subdirectory the walk leaves unlisted goes to `at_limit`, as
    fsspec finds it, but not what is in it.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        # The paths in each directory listed that are not done yet, for strategies
        # finishing a directory after all in it, as a deletion does. A directory with
        # a failure in it never reaches zero, and nor do the directories above it.
        self.remaining: dict[str, int] = {}
        self.parents: dict[str, str] = {}

        # The directories whose listing goes on, and the subdirectories counted in
        # each: a directory is done only once listed to its last part.
        self.listing: dict[str, set[str]] = {}

    @abstractmethod
    def add(self, listing: _Listing) -> list[Any] | None:
        """Queue the jobs for a part of a directory's listing, listed without error.

        It returns what is done at once, if anything, e.g. a directory found gone.
        """

    def glob(self, pattern: str | Sequence[str]) -> Operation[Any, Any]:
        return _tree_glob(self.fs, pattern, maxdepth=self.maxdepth)

    def unlisted(self, directory: str) -> bool:
        """Whether a root directory is at the depth limit already: a match, at 1."""
        return self.maxdepth == 1 and directory in self.matched

    def source(self, roots: _Roots) -> AsyncGenerator[_Listing] | None:
        """The listings of the root directories, if any, walked at once."""
        directories = [
            d["name"] for d in roots.directories if not self.unlisted(d["name"])
        ]
        if not directories:
            return None
        return _chunks(
            self.fs,
            directories,
            maxdepth=self.maxdepth,
            matched=self.matched,
            stopping=self.op.cancelling,
        )

    async def begin(
        self, roots: _Roots, source: AsyncGenerator[_Listing] | None
    ) -> list[Any]:
        """Queue the files' jobs, and list the directories: the first step.

        A single root of unknown kind is among the directories: if it is a file,
        its listing says so, and its jobs are queued instead.
        """
        for directory in roots.directories:
            if self.unlisted(directory["name"]):
                self.at_limit(directory["name"])

        try:
            return await super().begin(roots, source)
        except NotADirectoryError as e:
            # The root is a file: its listing's entry, which the walk noted.
            self.file_root(getattr(e, "entry", None) or {"name": e.args[0]})
            return []

    def fed(self, item: _Listing) -> list[Any]:
        self.current = item.directory
        if item.error is not None:
            self.errors += 1
            return [item.error]

        if self.maxdepth is not None and item.depth >= self.maxdepth:
            # The walk lists none of these subdirectories: fsspec finds them, but
            # nothing in them.
            for entry in item.entries:
                if entry["type"] == "directory":
                    self.at_limit(entry["name"].rstrip("/"))

        return self.add(item) or []

    # A hook, not abstract: only a deletion acts on a directory it does not list.
    def at_limit(self, directory: str) -> None:
        """Queue the jobs for a directory at the depth limit, found but not listed."""

    def track(self, listing: _Listing) -> None:
        """Count the paths in a part of a directory's listing, for `child_done`.

        A storage may list a directory twice, e.g. as a prefix and as its marker
        object, maybe in two parts. Each directory counts once, as the walk lists
        it once. A file is listed once: its name is not kept.
        """
        directory = listing.directory
        if listing.first:
            self.remaining[directory] = 0
            self.listing[directory] = set()

        subdirs = self.listing[directory]
        for entry in listing.entries:
            name = entry["name"].rstrip("/")
            if entry["type"] == "directory":
                if name in subdirs:
                    continue
                subdirs.add(name)
            self.remaining[directory] += 1
            self.parents[name] = directory

        if listing.last:
            del self.listing[directory]

    def child_done(self, path: str) -> str | None:
        """Count a path done, and return its directory if all in it is now done.

        A path done twice, as one listed twice is, counts once.
        """
        parent = self.parents.pop(path, None)
        if parent is None:
            return None
        self.remaining[parent] -= 1
        return parent if self.finished(parent) else None

    def finished(self, directory: str) -> bool:
        """Whether all in a directory is done: listed to its last part, each path done."""
        return directory not in self.listing and self.remaining.get(directory) == 0
