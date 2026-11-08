# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Where a recursive copy or move puts each path, as fsspec's ``copy`` does.

`_Placing` is mixed into the jobs of a copy, such as `tree_cp`'s or
`flat_cp`'s. It finds the roots, and the place of each path under them. A
family expands patterns with its own glob, in ``expand``.

This module depends on `jobs` and `paths`.
"""

from __future__ import annotations

from abc import abstractmethod
from glob import has_magic
from typing import TYPE_CHECKING

from fsspec.utils import common_prefix

from .jobs import _Roots
from .paths import _trim

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .types import _Info


class _Placing:
    """Places the paths of a copy or a move, as fsspec's ``copy`` places them.

    A path under ``prefix`` goes to the same place under ``base``. For a list,
    a path under a root of ``places`` goes there instead. A subclass sets
    ``path1``, ``path2`` and ``info``, and calls `_placing` first. ``info``
    describes the destination too, unless `_placing` is given its own.
    """

    verb = "copy"
    doing = "Copying"
    path1: str | Sequence[str]
    path2: str
    info: _Info

    def _placing(
        self, destination_info: _Info | None = None, *, same_storage: bool = True
    ) -> None:
        """Start placing paths.

        A copy between two storages passes the destination's ``info``, and
        ``same_storage=False``: a path may then go to the same path, or under
        it, as that is another storage's.
        """
        self.destination_info = destination_info or self.info
        self.same_storage = same_storage

        self.prefix = ""
        self.base = ""

        # For a list: where each root goes, by the root.
        self.places: dict[str, str] = {}

        # The destinations found missing, which need no asking again, e.g.
        # by a move renaming a root onto one.
        self.missing: set[str] = set()

    @abstractmethod
    async def expand(self, pattern: str | Sequence[str]) -> _Roots:
        """The roots matching glob patterns, found with the family's glob."""

    async def prepare(self) -> _Roots | None:
        """Set where the paths go, as fsspec's ``copy`` places them."""
        destination = _trim(self.path2)

        if not isinstance(self.path1, str):
            roots = await self._place(destination)
            sources = [*roots.folders, *roots.files]
        elif has_magic(self.path1):
            roots = await self.expand(self.path1)
            sources = [*roots.folders, *roots.files]
            if not sources:
                return roots  # Only errors, to yield.
            self.prefix = common_prefix(sources)
            if sources == list(roots.files) and len(sources) == 1:
                # A single file goes into the destination.
                self.prefix = self.prefix.rpartition("/")[0]
            self.base = destination
        else:
            root = _trim(self.path1)
            if root == destination and self.same_storage:
                return None
            roots = _Roots([root])
            sources = [root]
            self.prefix = root
            self.base = await self._base(root, destination)

        for source in sources if self.same_storage else ():
            target = self.target(source)
            if target == source and len(sources) == 1:
                return None
            if not source or target == source or target.startswith(source + "/"):
                raise ValueError(
                    f"Cannot {self.verb} {source!r} into itself: {target!r}"
                )

        return roots

    async def _place(self, destination: str) -> _Roots:
        """Place each root of a list into ``destination``, under its name.

        Two roots of the same name would go to the same place. As GNU's cp
        does, the first, in path order, goes, and each other is an error. That
        error is the root's own: what is in it is not copied either.
        """
        roots = await self.expand(self.path1)
        folders = set(roots.folders)
        placed: dict[str, str] = {}
        errors = list(roots.errors)

        for source in sorted([*roots.folders, *roots.files]):
            name = source.rpartition("/")[2]
            target = f"{destination}/{name}" if destination else name
            if (other := placed.get(target)) is not None:
                error = FileExistsError(target)
                error.add_note(
                    f"{self.doing} {source!r} to {target!r} skipped: {other!r} goes"
                    " there too."
                )
                errors.append(error)
                continue

            placed[target] = source
            self.places[source] = target

        return _Roots(
            [s for s in self.places if s in folders],
            [s for s in self.places if s not in folders],
            errors,
        )

    async def _base(self, root: str, destination: str) -> str:
        """Where ``root``, a single path, goes: into ``destination``, or onto it."""
        if self.path2.endswith("/"):
            into = True
        else:
            try:
                entry = await self.destination_info(destination)
                into = entry["type"] == "directory"
            except FileNotFoundError:
                self.missing.add(destination)
                into = False

        if not into or self.path1.endswith("/"):
            return destination

        name = root.rsplit("/", 1)[-1]
        return f"{destination}/{name}" if destination else name

    def target(self, path: str) -> str:
        """Where a path goes."""
        if self.places:
            # The root it is under, found from the path up.
            root = path
            while root not in self.places and "/" in root:
                root = root.rpartition("/")[0]
            return self.places[root] + path[len(root) :]

        if self.prefix:
            return self.base + path[len(self.prefix) :]
        return f"{self.base}/{path}" if self.base else path
