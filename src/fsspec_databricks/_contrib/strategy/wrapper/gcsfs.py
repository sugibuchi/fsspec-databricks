# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""gcsfs's ``GCSFileSystem``, wrapped as a `FlatFileSystem`, listing by prefix and deleting in full batches.

`GcsfsWrapper` uses what gcsfs has beside fsspec's API:

- its recursive ``_ils`` gives gcsfs's ``_find`` the prefix, which lists only
  the objects starting with it: for the glob "data/2024-*", those of
  "data/2024-", not all of "data/". gcsfs still gathers every page before
  answering, so the listing is held whole, not streamed.
- its ``_rm_files`` deletes a batch by gcsfs's own ``_rm_files``, one batch
  request of up to 100 objects, GCS's most. gcsfs's ``_rm``, which the generic
  wrapper calls, sends batches of 20 since gcsfs 2026's ``ExtendedGcsFileSystem``,
  and on a bucket with a hierarchical namespace first asks each key's info.

Its copies are the generic one's, as gcsfs's ``objects.rewrite`` asks nothing
first.

On a bucket with a hierarchical namespace, folders are objects of their own,
which the flat strategies never delete: a recursive ``rm`` or ``mv`` leaves
them. Such a bucket suits `AsyncTreeWrapper`, whose ``_rmdir`` is gcsfs's,
deleting a folder. A filesystem serving both kinds of bucket is two under a
router.

It imports nothing of gcsfs: it calls the wrapped filesystem's own methods.
Those are private, as of gcsfs 2026.x: where ``_find`` takes no ``prefix``, or
``_rm_files`` is missing, it lists or deletes as the generic wrapper does.

This module depends on `flat`.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from .flat import AsyncFlatWrapper

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = ["GcsfsWrapper"]

_BATCH = 100
"""The most calls one GCS batch request takes."""


class GcsfsWrapper(AsyncFlatWrapper):
    """A gcsfs filesystem as a `FlatFileSystem`, listing by prefix and deleting in full batches.

    It takes what `AsyncFlatWrapper` takes. ``rm_batch_size`` is best a
    multiple of 100, the most objects one batch request deletes. It suits
    buckets without a hierarchical namespace.

    Examples
    --------
    >>> flat = GcsfsWrapper(
    ...     gcsfs.GCSFileSystem(),
    ...     rm_batch_size=100,
    ...     operation_concurrency=32,
    ...     io_concurrency=8,
    ...     storage_profile=known_implementations["gcsfs.core.GCSFileSystem"]["storage_profile"],
    ... )
    """

    def _ils(
        self, path: str, recursive: bool = False, prefix: str = "", **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        if recursive and path and _takes_prefix(self.fs._find):
            return self._objects(path, prefix)
        else:
            # The buckets, or one directory: as the generic wrapper lists them.
            return super()._ils(path, recursive=recursive, prefix=prefix, **kwargs)

    async def _objects(self, path: str, prefix: str) -> AsyncIterator[dict[str, Any]]:
        """Every object starting with ``path/prefix``, in name order, listed by that prefix."""
        try:
            found = await self.fs._find(path, detail=True, prefix=prefix)
        except FileNotFoundError:
            return  # A missing bucket lists nothing, as a missing prefix does.
        for entry in sorted(found.values(), key=lambda e: e["name"]):
            yield entry

    async def _rm_files(self, paths: list[str]) -> dict[str, Exception] | None:
        # gcsfs batches deletions only on Google's own endpoint: elsewhere, e.g. on
        # an emulator, its _rm deletes one object per request.
        rm_files = getattr(self.fs, "_rm_files", None)
        if rm_files is None or not getattr(self.fs, "on_google", False):
            return await super()._rm_files(paths)

        refused: dict[str, Exception] = {}
        for start in range(0, len(paths), _BATCH):
            batch = list(paths[start : start + _BATCH])
            refused.update(await self._refused(batch, await rm_files(batch)))
        return refused

    async def _refused(
        self, batch: list[str], outcomes: list[Any]
    ) -> dict[str, Exception]:
        """The keys of a batch not deleted, each with an error, from gcsfs's outcomes.

        gcsfs answers each object deleted by its path, and each other by an
        OSError naming no key, a missing object's 404 among them. So a key not
        deleted is asked for only when an error other than a 404 came back:
        still there, it was refused.
        """
        deleted = {o for o in outcomes if isinstance(o, str)}
        errors = [o for o in outcomes if isinstance(o, Exception) and not _absent(o)]
        if not errors:
            return {}  # Each key not deleted was missing: no error.

        refused: dict[str, Exception] = {}
        for key in batch:
            if key in deleted:
                continue
            try:
                await self.fs._info(key)
            except FileNotFoundError:
                continue
            # gcsfs ties no error to its key: each key refused gets the batch's first.
            refused[key] = OSError(f"{key} was not deleted: {errors[0]}")
        return refused


def _absent(error: Exception) -> bool:
    """Whether gcsfs's error of a batch deletion is a missing object's, which is no error."""
    text = str(error)
    return "No such object" in text or '"code": 404' in text or ": 404" in text


def _takes_prefix(find: Any) -> bool:
    """Whether a ``_find`` lists by a prefix, as gcsfs's does: fsspec's passes it on to ``_ls``."""
    try:
        return "prefix" in inspect.signature(find).parameters
    except (TypeError, ValueError):
        return False
