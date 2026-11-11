# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The listing under the flat strategies: a prefix's keys, and the directories they imply.

A flat storage lists the keys under a prefix, at any depth, but no directories. A
directory exists as the common prefix of keys, or as an empty marker object whose
key ends with "/". fsspec's ``_find`` and ``_glob`` answer with directories too, so
the strategies derive them from the keys, as `_dirs` does.

The strategies list by a raw prefix, e.g. "data/2024-", as `_listing` lists
it: a recursive ``_ils`` of its directory, narrowed to the rest of its name.
The listing holds no permit of the concurrency limit: its pages come one after
another, and the strategy never sees them. Only the other requests, such as an
``info``, hold one.

This module depends on nothing else in the package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _listing(fs: Any, prefix: str) -> AsyncIterator[dict[str, Any]]:
    """Every key starting with ``prefix``, raw text, in name order.

    It is the recursive ``_ils`` of the prefix's directory, narrowed to the rest
    of its name: "data/2024-" lists "data" with the prefix "2024-", and "data/"
    all of "data".
    """
    path, _, name = prefix.rpartition("/")
    return fs._ils(path, recursive=True, prefix=name)


def _dir(name: str) -> dict[str, Any]:
    """A directory's entry, as fsspec's filesystems derive one for an object store."""
    return {"name": name, "size": 0, "type": "directory"}


def _dirs(name: str) -> list[str]:
    """The directories a key implies, the shallowest first.

    They are the key's parents, and the key itself if it is a directory's marker,
    ending with "/".
    """
    parts = name.rstrip("/").split("/")
    return ["/".join(parts[:i]) for i in range(1, len(parts) + name.endswith("/"))]


def _new(directories: list[str], before: list[str]) -> list[str]:
    """The directories of a key not seen yet, given those of the key listed before.

    The keys come in order, so those in a directory come together: a directory seen
    before is one of the key before's, which the two keys start with.
    """
    shared = 0
    while (
        shared < min(len(directories), len(before))
        and directories[shared] == before[shared]
    ):
        shared += 1
    return directories[shared:]


async def _close(entries: AsyncIterator[dict[str, Any]]) -> None:
    """Close a listing left early, if it can be: an async generator can."""
    if (aclose := getattr(entries, "aclose", None)) is not None:
        await aclose()
