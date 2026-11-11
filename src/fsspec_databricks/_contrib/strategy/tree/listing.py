# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive listing made of one listing per directory, several at a time.

A `TreeFileSystem`'s ``_ils`` lists all below a directory with
``recursive=True``, depth first or in name order. A storage with a recursive
listing of its own answers with it. One without answers with `_tree_ils`, which
lists a directory per request, as ``_ils`` does without ``recursive``.

It yields the entries depth first, as a walk visits them: each directory's
entries in the order its listing gives them, and all below a subdirectory right
after its entry. So it lists the directories in that order too, but starts the
next ones ahead, as their entries arrive, so that their listings overlap.

This module depends on `paths` of `common`.
"""

from __future__ import annotations

import asyncio
import heapq
from collections import deque
from typing import TYPE_CHECKING, Any

from ..common.paths import _trim

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

__all__: list[str] = []


_AHEAD = 32
"""The most directories listed ahead of the one being yielded."""

_BATCH = 1000
"""The most entries of a directory read ahead of those yielded: a page's worth."""


async def _tree_ils(
    fs: Any, path: str, **ils_kwargs: Any
) -> AsyncGenerator[dict[str, Any]]:
    """List all below a directory depth first, by one ``_ils`` per directory.

    It yields what a recursive ``_ils`` yields: each entry below ``path``, at any
    depth, depth first. A directory's entries come in the order its ``_ils``
    gives them, each subdirectory's followed by all below it. A
    file lists itself, as ``_ils`` lists it. A directory listed twice, e.g. as a
    prefix and as its marker object, is yielded twice, but listed once.

    It lists the directories that come next while it yields, up to
    `_AHEAD` of them, the nearest first. Each such listing holds a permit of the
    filesystem's ``operation_limit`` while it reads a batch of entries, a page's
    worth: what it read waits for its turn, holding no permit. The directory
    being yielded is listed under the permit its caller holds, as for any
    ``_ils``. So it needs no permit more to go on.

    A directory below ``path`` that fails to list raises its error, noted with
    its path, and ends the listing: a recursive listing cannot go on past a
    failure. A missing ``path`` raises FileNotFoundError, when the iteration
    starts, as its own ``_ils`` raises it.

    ``ils_kwargs`` go to each ``_ils``.
    """
    root = _trim(path)
    ahead = _Ahead(fs, ils_kwargs)
    # The directories being yielded, the root first: each is yielding one of the next.
    stack = [ahead.take(root)]

    try:
        while stack:
            listing = stack[-1]
            try:
                item = await listing.next()
            except Exception as e:
                if len(stack) > 1:  # The root's error is the listing's own.
                    e.add_note(f"Listing {listing.directory!r} failed.")
                raise
            if item is None:
                stack.pop()
                continue

            entry, subdirectory = item
            if entry["name"].rstrip("/") == listing.directory:
                # A file lists itself, as the root may. A directory may list its
                # own marker: it is no entry below it.
                if len(stack) == 1 and entry["type"] != "directory":
                    yield entry
                continue

            yield entry
            if subdirectory is not None:
                stack.append(ahead.take(subdirectory))
    finally:
        await ahead.close(stack)


class _Ahead:
    """The directories found but not yielded yet, some of them listed ahead."""

    def __init__(self, fs: Any, ils_kwargs: dict[str, Any]) -> None:
        self.fs = fs
        self.ils_kwargs = ils_kwargs
        self.found: dict[str, _DirListing] = {}
        # The directories found and not listed yet, the nearest first: depth
        # first, a path comes before another as its parts do.
        self.queue: list[tuple[list[str], str]] = []
        self.listed = 0  # The directories listed ahead, not taken yet.

    def add(self, directory: str) -> None:
        """Note a subdirectory read in a listing, to list it ahead."""
        self.found[directory] = _DirListing(self, directory)
        heapq.heappush(self.queue, (directory.split("/"), directory))
        self.start()

    def start(self) -> None:
        """Start listing the nearest directories found, up to `_AHEAD` at a time."""
        while self.queue and self.listed < _AHEAD:
            _, directory = heapq.heappop(self.queue)
            listing = self.found.get(directory)
            if listing is not None:  # Else taken already, to be yielded.
                listing.task = asyncio.ensure_future(listing.read_ahead())
                self.listed += 1

    def take(self, directory: str) -> _DirListing:
        """The listing of the directory to yield next, listed ahead or not."""
        listing = self.found.pop(directory, None)
        if listing is None:
            return _DirListing(self, directory)

        if listing.task is not None:
            self.listed -= 1
            self.start()
        return listing

    async def close(self, stack: list[_DirListing]) -> None:
        """Stop the listings ahead, and close those of every directory left."""
        listings = [*stack, *self.found.values()]
        tasks = [lst.task for lst in listings if lst.task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for listing in listings:
            await listing.close()


class _DirListing:
    """A directory's ``_ils``, read a batch at a time, ahead of its turn or in it."""

    def __init__(self, ahead: _Ahead, directory: str) -> None:
        self.ahead = ahead
        self.directory = directory
        # Each entry read, with the path of a subdirectory it is the first entry of.
        self.entries: deque[tuple[dict[str, Any], str | None]] = deque()
        self.subdirectories: set[str] = set()
        self.iterator: AsyncIterator[dict[str, Any]] | None = None
        self.done = False
        self.error: Exception | None = None
        self.task: asyncio.Task[None] | None = None
        self.reading = False  # Whether the task ahead holds a permit, reading.

    async def next(self) -> tuple[dict[str, Any], str | None] | None:
        """The next entry, read under the caller's permit if need be, or None at the end."""
        while not self.entries:
            if self.task is not None:
                task, self.task = self.task, None
                if self.reading:
                    await task  # It reads a batch: wait for it.
                else:
                    # Still waiting for a permit: read under the caller's instead.
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            elif self.error is not None:
                raise self.error
            elif self.done:
                return None
            else:
                await self.read()
        return self.entries.popleft()

    async def read_ahead(self) -> None:
        """Read a batch ahead of the directory's turn, under a permit of its own."""
        async with self.ahead.fs.operation_limit:
            self.reading = True
            try:
                await self.read()
            finally:
                self.reading = False

    async def read(self) -> None:
        """Read a batch of entries, noting the subdirectories in it to list ahead."""
        if self.iterator is None:
            self.iterator = self.ahead.fs._ils(self.directory, **self.ahead.ils_kwargs)

        try:
            for _ in range(_BATCH):
                entry = await anext(self.iterator)
                self.entries.append((entry, self.subdirectory(entry)))
        except StopAsyncIteration:
            self.done = True
        except Exception as e:
            self.error = e

        if self.done or self.error is not None:
            await self.close()

    def subdirectory(self, entry: dict[str, Any]) -> str | None:
        """The entry's path, if a subdirectory not read before: it is listed once."""
        name = entry["name"].rstrip("/")
        if (
            entry["type"] != "directory"
            or name == self.directory
            or name in self.subdirectories
        ):
            return None
        self.subdirectories.add(name)
        self.ahead.add(name)
        return name

    async def close(self) -> None:
        iterator, self.iterator = self.iterator, None
        if iterator is not None and (aclose := getattr(iterator, "aclose", None)):
            await aclose()
