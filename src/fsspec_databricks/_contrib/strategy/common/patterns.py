# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Glob patterns, matched as fsspec's ``_glob`` matches them.

Both globs, `_tree_glob` and `_flat_glob`, match with `_Search`, so the two never
answer differently. A `_Search` also tells which directories the pattern can still
match in, as `_tree_glob` prunes by directory.

To decide, a directory's path is matched with the pattern's segments, one by one.
A ``**`` matches any number of segments, so only those before the first ``**``
can rule a directory out. Without a ``**``, a directory as deep as the pattern
is ruled out too, as nothing below it can match.

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


async def _infos(
    fs: Any,
    paths: list[str],
    op: Operation[dict[str, Any], GlobState],
    found: set[str],
) -> AsyncGenerator[dict[str, Any]]:
    """The ``_info`` of each pattern without wildcards that exists, noted in ``found``.

    They run at once, each holding a permit of the filesystem's
    ``operation_limit``: a list of plain paths may give many. The entries come
    as the answers arrive. Once the operation is cancelled, no new ``_info``
    starts, but those running are yielded.
    """
    requests: _Queue[str] = _Queue(fs.operation_limit)
    requests.waiting.extend(dict.fromkeys(paths))

    async def start() -> None:
        if op.cancelling():
            requests.waiting.clear()
        await requests.start(fs._info)

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
                    directories_listed=state.directories_listed,
                    matched=state.matched + 1,
                )
                yield entry
    finally:
        await requests.close()


def _inside(path: str, directory: str) -> bool:
    """Whether ``path`` is below ``directory``, "" being the top of the storage."""
    return directory == "" or path.startswith(directory + "/")


class _Prefix:
    """A pattern's segments before its first ``**``, which decide the directories to list."""

    def __init__(self, segments: list[str]) -> None:
        # Without a **, the pattern is as deep as its segments. With one, it can
        # match at any depth: only the segments before it can rule a directory out.
        self.bounded = "**" not in segments
        if not self.bounded:
            segments = segments[: segments.index("**")]
        self.segments = [re.compile(glob_translate(s)) for s in segments]

    def everything(self, relative: list[str]) -> bool:
        """Whether the pattern prunes nothing below a directory, at that path."""
        return (
            not self.bounded
            and len(relative) >= len(self.segments)
            and self.alive(relative)
        )

    def alive(self, relative: list[str]) -> bool:
        """Whether the pattern can still match below a directory, at that path."""
        if self.bounded and len(relative) >= len(self.segments):
            return False
        else:
            return all(
                s.match(n) for s, n in zip(self.segments, relative, strict=False)
            )


class _Search:
    """One glob pattern, split and compiled.

    It answers as a walk from its own literal root would: it matches only
    below that root, or the root itself, as deep as the pattern goes. So a walk
    from a directory above serves it too.
    """

    def __init__(
        self,
        path: str,
        ends_with_sep: bool,
        withdirs: bool,
        maxdepth: int | None,
    ) -> None:
        self.withdirs = withdirs

        # Split as fsspec does: the literal directory before the first wildcard,
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

    def depth_from(self, directory: str) -> int | None:
        """How deep to walk from ``directory``, at or above the root, for this pattern."""
        if self.depth is None:
            return None
        between = self.root.count("/") + 1 if self.root else 0
        above = directory.count("/") + 1 if directory else 0
        return self.depth + between - above

    def _level(self, name: str) -> int | None:
        """How far below the root a path is, 0 for the root, or None if not below."""
        if name == self.root:
            return 0
        if not _inside(name, self.root):
            return None
        else:
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
        else:
            return (
                self.matches(name + "/" if is_dir and self.slash_dirs else name)
                is not None
            )

    def descend(self, entry: dict[str, Any]) -> bool:
        name = entry["name"].rstrip("/")
        if self.root.startswith(name + "/"):
            return True  # On the way down to the root.

        level = self._level(name)
        if level is None or (self.depth is not None and level >= self.depth):
            return False
        else:
            return level == 0 or self.prefix.alive(name[self.start :].split("/"))

    def whole(self, entry: dict[str, Any]) -> bool:
        """Whether every directory below a directory's entry is one to descend into.

        So it is at or below the pattern's first ``**``, when no depth limits
        it: a recursive listing of the directory lists what a walk would.
        """
        name = entry["name"].rstrip("/")
        level = self._level(name)
        if level is None or self.depth is not None:
            return False
        else:
            return self.prefix.everything(
                name[self.start :].split("/") if level else []
            )


def _depth(path: str, relative: str, maxdepth: int | None) -> int | None:
    """How deep to walk below the root, as fsspec's ``_glob`` computes it."""
    depth = relative.count("/") + 1
    if "**" not in path:
        return depth
    if maxdepth is None:
        return None
    else:
        return depth - path[path.find("**") :].count("/") - 1 + maxdepth
