# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The listing under the flat strategies: a prefix's keys, and the folders they imply.

A flat storage lists the keys under a prefix, at any depth, but no folders. A
folder exists as the common prefix of keys, or as an empty marker object whose
key ends with "/". fsspec's ``_find`` and ``_glob`` answer with folders too, so
the strategies derive them from the keys, as `_folders` does.

The listing, ``iexpand``, holds no permit of the concurrency limit: its pages come
one after another, and the strategy never sees them. Only the other requests,
such as an ``info``, hold one.

This module depends on nothing else in the package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _folder(name: str) -> dict[str, Any]:
    """A folder's entry, as fsspec's filesystems derive one for an object store."""
    return {"name": name, "size": 0, "type": "directory"}


def _folders(name: str) -> list[str]:
    """The folders a key implies, the shallowest first.

    They are the key's parents, and the key itself if it is a folder's marker,
    ending with "/".
    """
    parts = name.rstrip("/").split("/")
    return ["/".join(parts[:i]) for i in range(1, len(parts) + name.endswith("/"))]


def _new(folders: list[str], before: list[str]) -> list[str]:
    """The folders of a key not seen yet, given those of the key listed before.

    The keys come in order, so those in a folder come together: a folder seen
    before is one of the key before's, which the two keys start with.
    """
    shared = 0
    while shared < min(len(folders), len(before)) and folders[shared] == before[shared]:
        shared += 1
    return folders[shared:]


async def _close(entries: AsyncIterator[dict[str, Any]]) -> None:
    """Close a listing left early, if it can be: an async generator can."""
    if (aclose := getattr(entries, "aclose", None)) is not None:
        await aclose()
