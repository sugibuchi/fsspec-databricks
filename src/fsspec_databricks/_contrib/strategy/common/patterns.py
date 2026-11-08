# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Glob patterns, matched as fsspec's ``_glob`` matches them.

Both globs, `tree_glob` and `flat_glob`, match with `_Search`, so the two never
answer differently. A `_Search` also tells which folders the pattern can still
match in, as `tree_glob` prunes by folder.

To decide, a folder's path is matched with the pattern's segments, one by one.
A ``**`` matches any number of segments, so only those before the first ``**``
can rule a folder out. Without a ``**``, nor can a folder as deep as the
pattern, as nothing below it can match.

This module depends on `jobs` and `paths`.
"""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING, Any

from fsspec.utils import glob_translate

from fsspec_alt_strategies.states import GlobState

from .jobs import _Queue
from .paths import _trim

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from .operation import Operation
    from .types import _Info


async def _infos(
    info: _Info,
    paths: list[str],
    concurrency_limit: asyncio.Semaphore,
    op: Operation[dict[str, Any], GlobState],
    found: set[str],
) -> AsyncGenerator[dict[str, Any]]:
    """The ``info`` of each pattern without wildcards that exists, noted in ``found``.

    They run at once, each holding a permit of ``concurrency_limit``: a list of
    plain paths may give many. The entries come as the answers arrive. Once the
    operation is cancelled, no new ``info`` starts, but those running are
    yielded.
    """
    requests: _Queue[str] = _Queue(concurrency_limit)
    requests.waiting.extend(dict.fromkeys(paths))

    async def start() -> None:
        if op.cancelling():
            requests.waiting.clear()
        await requests.start(info)

    try:
        await start()

        while pending := requests.pending():
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            entries = []
            for task in done:
                if (path := requests.pop(task)) is None:
                    continue
                try:
                    entries.append((path, task.result()))
                except FileNotFoundError:
                    continue

            # Start the next before yielding, so that they run meanwhile.
            await start()

            for path, entry in entries:
                found.add(entry["name"].rstrip("/"))
                state = op.state
                op.state = GlobState(
                    errors=state.errors,
                    current=path,
                    folders_listed=state.folders_listed,
                    matched=state.matched + 1,
                )
                yield entry
    finally:
        await requests.close()


def _inside(path: str, folder: str) -> bool:
    """Whether ``path`` is below ``folder``, "" being the top of the storage."""
    return folder == "" or path.startswith(folder + "/")


class _Prefix:
    """A pattern's segments before its first ``**``, which decide the folders to list."""

    def __init__(self, segments: list[str]) -> None:
        # Without a **, the pattern is as deep as its segments. With one, it can
        # match at any depth: only the segments before it can rule a folder out.
        self.bounded = "**" not in segments
        if not self.bounded:
            segments = segments[: segments.index("**")]
        self.segments = [re.compile(glob_translate(s)) for s in segments]

    def alive(self, relative: list[str]) -> bool:
        """Whether the pattern can still match below a folder, at that path."""
        if self.bounded and len(relative) >= len(self.segments):
            return False
        return all(s.match(n) for s, n in zip(self.segments, relative, strict=False))


class _Search:
    """One glob pattern, split and compiled.

    It answers as a walk from its own literal root would: it matches only
    below that root, or the root itself, as deep as the pattern goes. So a walk
    from a folder above serves it too.
    """

    def __init__(
        self,
        path: str,
        ends_with_sep: bool,
        withdirs: bool,
        maxdepth: int | None,
    ) -> None:
        self.withdirs = withdirs

        # Split as fsspec does: the literal folder before the first wildcard,
        # and the pattern below it.
        first_magic = min(i for i in (path.find(c) for c in "*?[") if i >= 0)
        slash = path.rfind("/", 0, first_magic)
        relative = path[slash + 1 :]
        self.root = _trim(path[: slash + 1])
        self.start = len(self.root) + 1 if self.root else 0
        self.depth = _depth(path, relative, maxdepth)
        self.prefix = _Prefix(relative.split("/"))

        # The matches come from fsspec's own regular expression, and the prefix
        # only prunes. So pruning can never change them.
        regex = glob_translate(path + ("/" if ends_with_sep else ""))
        self.matches = re.compile(regex).match
        self.slash_dirs = ends_with_sep or path.endswith("/**")

    def depth_from(self, folder: str) -> int | None:
        """How deep to walk from ``folder``, at or above the root, for this pattern."""
        if self.depth is None:
            return None
        between = self.root.count("/") + 1 if self.root else 0
        above = folder.count("/") + 1 if folder else 0
        return self.depth + between - above

    def _level(self, name: str) -> int | None:
        """How far below the root a path is, 0 for the root, or None if not below."""
        if name == self.root:
            return 0
        if not _inside(name, self.root):
            return None
        return name[self.start :].count("/") + 1

    def matched(self, entry: dict[str, Any]) -> bool:
        is_dir = entry["type"] == "directory"
        if is_dir and not self.withdirs:
            return False

        name = entry["name"].rstrip("/")
        level = self._level(name)
        if (
            level is None
            or (level == 0 and not is_dir)
            or (self.depth is not None and level > self.depth)
        ):
            return False

        return (
            self.matches(name + "/" if is_dir and self.slash_dirs else name) is not None
        )

    def descend(self, entry: dict[str, Any]) -> bool:
        name = entry["name"].rstrip("/")
        if self.root.startswith(name + "/"):
            return True  # On the way down to the root.

        level = self._level(name)
        if level is None or (self.depth is not None and level >= self.depth):
            return False
        return level == 0 or self.prefix.alive(name[self.start :].split("/"))


def _depth(path: str, relative: str, maxdepth: int | None) -> int | None:
    """How deep to walk below the root, as fsspec's ``_glob`` computes it."""
    depth = relative.count("/") + 1
    if "**" not in path:
        return depth
    if maxdepth is None:
        return None
    return depth - path[path.find("**") :].count("/") - 1 + maxdepth
