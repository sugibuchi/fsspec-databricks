# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""adlfs's ``AzureBlobFileSystem``, wrapped as a `TreeFileSystem`, listing a tree in one request.

adlfs lists one directory per request, with a delimiter, so it is wrapped as a
tree, with or without a hierarchical namespace. The generic `AsyncTreeWrapper`
lists a tree recursively a directory at a time, by adlfs's ``_ls``.
`AdlfsWrapper` lists it by one ListBlobs without a delimiter, from the Azure
SDK's container client that adlfs holds, a page at a time, each converted by
adlfs's own ``_details``. Measured on Azure, a find of 512 files in 340
directories took 1 request this way, against 342.

The blobs come sorted by the whole name without a hierarchical namespace, and
depth first with one, as the tree strategies take a recursive listing. A
directory with no marker blob is left out: the strategies tell it from the
paths below it.

Its ``_rmdir`` deletes a directory below a container, which adlfs's leaves,
so that a recursive ``rm`` leaves none with a hierarchical namespace.

Its copies are the generic one's: adlfs's copy asks nothing first. It imports
nothing of adlfs or of the Azure SDK: it calls the wrapped filesystem's own
objects. ``service_client`` and ``_details`` are private, as of adlfs 2026.x:
where one is missing, it lists as the generic wrapper does.

This module depends on `tree`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .tree import AsyncTreeWrapper

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = ["AdlfsWrapper"]


class AdlfsWrapper(AsyncTreeWrapper):
    """An adlfs filesystem as a `TreeFileSystem`, its trees listed by one request.

    It takes what `AsyncTreeWrapper` takes.

    Examples
    --------
    >>> tree = AdlfsWrapper(
    ...     adlfs.AzureBlobFileSystem(account_name="myaccount"),
    ...     operation_concurrency=32,
    ...     io_concurrency=8,
    ...     storage_profile=known_implementations["adlfs.spec.AzureBlobFileSystem"]["storage_profile"],
    ... )
    """

    async def _rmdir(self, path: str) -> None:
        # adlfs's _rmdir deletes containers alone: a directory below one would
        # stay, as a hierarchical namespace keeps it. Its blob is deleted as a
        # file's, which deletes an empty directory, or its marker without one.
        # One gone, or never marked, raises FileNotFoundError: deleted, to the
        # strategies.
        container, key, _ = self.fs.split_path(path)
        if key.strip("/"):
            await self.fs._rm_file(path)
        else:
            await self.fs._rmdir(container)

    def _ils(
        self, path: str, recursive: bool = False, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        container = self.fs.split_path(path)[0] if path.strip("/") else ""
        own = hasattr(self.fs, "service_client") and hasattr(self.fs, "_details")
        if recursive and container and own:
            return self._blobs(path)
        else:
            # The containers, or one directory: as the generic wrapper lists them.
            return super()._ils(path, recursive=recursive, **kwargs)

    async def _blobs(self, path: str) -> AsyncIterator[dict[str, Any]]:
        """All below ``path``, by one ListBlobs without a delimiter, a page at a time.

        A file lists itself, and a missing path raises FileNotFoundError, as
        ``_ils`` does: nothing listed, its info tells which.
        """
        container, key, _ = self.fs.split_path(path)
        prefix = f"{key.strip('/')}/" if key.strip("/") else ""
        listed = False
        try:
            client = self.fs.service_client.get_container_client(container)
            listing = client.list_blobs(include=["metadata"], name_starts_with=prefix)
            async for page in listing.by_page():
                blobs = [b async for b in page if b.name != prefix]
                for entry in await self.fs._details(blobs):
                    listed = True
                    yield _sized(entry)
        except Exception:
            if listed:
                raise
            # E.g. a missing container: its info raises FileNotFoundError.
            entry = await self.fs._info(path)
            if entry["type"] == "directory":
                raise
            yield entry  # A file lists itself.
            return

        if not listed and key.strip("/"):
            # An empty directory lists nothing; a file itself; a missing path raises.
            entry = await self.fs._info(path)
            if entry["type"] != "directory":
                yield entry


def _sized(entry: dict[str, Any]) -> dict[str, Any]:
    """An entry of adlfs, a directory's size 0, as `TreeFileSystem` lists it: adlfs gives None."""
    if entry["size"] is None:
        entry["size"] = 0
    return entry
