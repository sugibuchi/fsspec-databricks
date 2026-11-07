# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A file's data streamed from one storage to another, a block at a time.

`_streaming` makes the ``copy_file`` of a copy between two storages, such as
`tree_transfer`'s, from the two storages' ``open_async``.

This module depends on nothing else in the package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .types import _Request

_MiB = 1024 * 1024


def _streaming(
    open_source: Callable[..., Awaitable[Any]],
    open_destination: Callable[..., Awaitable[Any]],
    *,
    block_size: int = 8 * _MiB,
) -> _Request:
    """A ``copy_file(source, destination)`` streaming the data between two storages.

    Each storage's ``open_async`` serves, called as ``open(path, "rb")`` and
    ``open(path, "wb")``. The file objects are awaited, as those of
    ``open_async`` are. A block is read, then written, before the next is
    read. So a copy holds one block in memory, however large the file.

    A copy failing midway leaves what the destination's file object leaves
    on an exception in its ``async with`` block. An adaptive file aborts its
    upload, for example, while a plain one may keep the blocks written.
    """
    if block_size < 1:
        raise ValueError(f"block_size must be at least 1: {block_size}")

    async def copy_file(source: str, destination: str) -> None:
        reader = await open_source(source, "rb")
        async with reader:
            writer = await open_destination(destination, "wb")
            async with writer:
                while block := await reader.read(block_size):
                    await writer.write(block)

    return copy_file
