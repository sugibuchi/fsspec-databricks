# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A concurrent walk of a folder tree, a folder per listing request.

`tree_walk` lists folders breadth first, several at a time, and yields each
folder's ``(path, dirs, files)`` as its listing arrives, as fsspec's ``walk``
gives them. A ``descend`` predicate prunes the folders it lists, as glob needs.

Its engine, ``_chunks``, is the walk under the other tree strategies, such as
find, rm and du. It streams each folder's listing in parts, as ``ils`` yields
it, so that they act on a folder of hundreds of thousands of files as it is
listed. ``tree_walk`` gathers the parts, as fsspec's ``walk`` gives a folder
whole.

It suits storages that list one folder per request, and those whose folders
can be pruned. On object stores, one recursive listing usually costs fewer
requests: `fsspec.asyn.AsyncFileSystem._find` there.

The tree strategies take the storage's operations as functions, such as
``ils``, rather than a filesystem: see `common.types`.

This module depends on `jobs`, `operation` and `paths` of `common`.
"""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from fsspec_alt_strategies.states import WalkState

from ..common.jobs import _Queue
from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim, _triples

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Collection

    from ..common.types import _Ils

__all__ = ["WalkState", "tree_walk"]


def tree_walk(
    path: str,
    *,
    ils: _Ils,
    maxdepth: int | None = None,
    topdown: bool = True,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    on_error: Literal["omit", "raise"] | Callable[[Exception], object] | None = None,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState]:
    """Walk a folder tree, listing several folders at a time.

    It yields each folder listed as ``(path, dirs, files)``, as fsspec's
    ``walk`` does with ``detail=True``: ``path`` itself first, then each folder
    below it. ``dirs`` and ``files`` map the names in the folder to their
    ``ils`` entries. An empty folder yields empty ones. The folders come in the
    order their listings arrive, not in the tree's order. A folder that fails
    to list yields its error instead, and the walk goes on with the others.
    The error names its folder in its note, and as its ``folder`` attribute.
    ``on_error`` handles those errors as with fsspec's ``walk``, otherwise.

    With ``topdown``, a folder comes before those below it. Without, it comes
    after all of those below it, as fsspec's ``walk`` orders them. The
    listings run as concurrently either way, but bottom up holds back each
    folder until its subtree is done. So a walk bottom up that is cancelled
    leaves out the folders whose subtrees were not done.

    Once cancelled, it lists no new folder, and yields the folders of the
    listings in flight. Iterating it raises these errors, which end it:

    - FileNotFoundError, if ``path`` does not exist;
    - NotADirectoryError, if ``path`` is a file;
    - with ``on_error="raise"``, the error of the first folder that failed to
      list.

    Requests: one ``ils`` per folder walked, as ``concurrency_limit`` allows.

    Parameters
    ----------
    path : str
        The folder to start from, as ``ils`` takes it.
    ils : callable
        Lists a folder, called as ``ils(path)``: an async iterator of its
        entries, as fsspec's ``_ls`` gives them, e.g. the filesystem's
        ``_ils``.
    maxdepth : int, optional
        The deepest level to list: 1 lists only ``path``. No limit by default.
    topdown : bool
        Whether a folder comes before those below it, rather than after.
    descend : callable, optional
        Called with the ``ils`` entry of each subfolder found. The walk lists it
        only if this returns True. All by default.
    on_error : {"omit", "raise"} or callable, optional
        What to do with the error of a folder that fails to list: yield it, if
        None, as every strategy does; leave it out, with "omit"; raise it,
        ending the walk, with "raise"; or call this with it, leaving it out.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs. Share one to limit several
        operations, or files, together.

    Returns
    -------
    Operation
        Yielding ``(path, dirs, files)`` for each folder listed, or the error of
        a folder that failed to list. Its state is a `WalkState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1, or ``on_error`` is none of the above.

    See Also
    --------
    tree_find : The paths in a tree, from the same walk.

    Examples
    --------
    >>> limit = asyncio.Semaphore(16)  # Shared, e.g. by a filesystem's operations.
    >>> walk = tree_walk("data", ils=fs._ils, concurrency_limit=limit)
    >>> async with walk as folders:
    ...     async for folder in folders:
    ...         if isinstance(folder, Exception):
    ...             raise folder
    ...         path, dirs, files = folder
    ...         print(path, sorted(dirs), sorted(files))
    """
    _check_maxdepth(maxdepth)
    if not (on_error in (None, "omit", "raise") or callable(on_error)):
        raise ValueError(f"on_error must be 'omit', 'raise' or callable: {on_error!r}")

    async def run(
        op: Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState],
    ) -> AsyncGenerator[tuple[str, dict[str, Any], dict[str, Any]] | Exception]:
        listings = _listings(
            ils,
            path,
            maxdepth=maxdepth,
            topdown=topdown,
            descend=descend,
            raise_on_error=on_error == "raise",
            concurrency_limit=concurrency_limit,
            stopping=op.cancelling,
        )
        state = op.state

        async with aclosing(listings):
            async for listing in listings:
                op.state = state = WalkState(
                    errors=state.errors + (listing.error is not None),
                    current=listing.folder,
                    folders_listed=state.folders_listed + 1,
                    found=state.found + len(listing.entries),
                )

                if listing.error is None:
                    yield _triples(listing.folder, listing.entries, detail=True)
                elif on_error is None:
                    yield listing.error
                elif callable(on_error):
                    on_error(listing.error)
                # Else "omit": left out.

    return Operation(run, WalkState(folders_listed=0))


@dataclass(frozen=True, slots=True)
class _Listing:
    """A part of a folder's listing, as ``_chunks`` yields it, or all of it.

    ``depth`` is 1 for the folder the walk started from, 2 for its subfolders,
    and so on. ``entries`` are as ``ils`` gives them, without the folder itself,
    and empty if the listing failed with ``error``.

    A folder's listing comes in parts, each of at most ``chunk_size`` entries,
    as ``ils`` yields them: ``first`` on the first part, ``last`` on the last.
    So an empty folder is one part, both first and last, with no entries. A
    failed listing ends with its error, as its last part. ``_listings`` yields
    each folder whole: one part, both first and last.

    ``depth`` 0 is no listing, but the walk's root itself, as `_find_listings`
    yields it: ``entries`` hold only the root's ``info`` entry, under ``folder``.
    """

    folder: str
    depth: int
    entries: list[dict[str, Any]]
    error: Exception | None = None
    first: bool = True
    last: bool = True


_CHUNK_SIZE = 1000
"""The most entries of one part of a listing: a folder's are never all held."""


async def _chunks(
    ils: _Ils,
    path: str | Collection[str],
    *,
    maxdepth: int | None = None,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    raise_on_error: bool = False,
    concurrency_limit: asyncio.Semaphore,
    stopping: Callable[[], object] | None = None,
    chunk_size: int = _CHUNK_SIZE,
) -> AsyncGenerator[_Listing]:
    """Walk a folder tree, yielding each folder's listing in parts, as they come.

    It is the walk under the strategies acting on what they find, such as rm
    and find: a folder of hundreds of thousands of files is streamed, a part
    at a time, never held whole. A subfolder is listed as soon as its entry
    comes, while its parent's listing goes on.

    ``path`` may be several folders, e.g. a glob's matches, walked at once, each
    as a root. None may be inside another. A root's listing failing before its
    first part raises, as fsspec's walk does, since nothing has been done yet:
    FileNotFoundError, or NotADirectoryError for a file. Any other failed
    listing is yielded as its last part, with its error, and its subfolders not
    listed yet are not walked. With ``raise_on_error``, it raises instead.

    The parts of a folder come in order, but those of several folders mix, as
    the listings run at once. Each listing holds a permit of
    ``concurrency_limit`` while it runs.

    Once ``stopping`` returns true, no new listing starts, and those in flight
    stop after their current part: a folder stopped so has no last part.
    """
    _check_maxdepth(maxdepth)

    roots = {_trim(p) for p in ([path] if isinstance(path, str) else path)}
    # The folders to list, each with its depth.
    folders: _Queue[tuple[str, int]] = _Queue(concurrency_limit)
    # The parts listed, a few at a time: a listing waits while they are full.
    parts: asyncio.Queue[_Listing] = asyncio.Queue(maxsize=16)
    # The subfolders found so far in each folder being listed, each listed once.
    found: dict[str, set[str]] = {}

    def stopped() -> bool:
        return stopping is not None and bool(stopping())

    async def start() -> None:
        if stopped():
            folders.waiting.clear()
        await folders.start(
            lambda item: _produce(
                ils, *item, item[0] in roots, parts, chunk_size, stopped
            )
        )

    folders.waiting.extend((root, 1) for root in sorted(roots))
    await start()

    try:
        while folders.waiting or folders.running or not parts.empty():
            if (part := await _next_part(parts, folders)) is None:
                await start()  # A listing ended, or a permit came: start more.
                continue

            if part.error is not None:
                # Raised here, it stops the walk before another listing starts.
                if (part.folder in roots and part.first) or raise_on_error:
                    raise part.error
                _note(part.error, part.folder)
            elif maxdepth is None or part.depth < maxdepth:
                folders.waiting.extend(_new_subfolders(part, descend, found))

            if part.last:
                found.pop(part.folder, None)

            # Start the next listings before yielding, so that they run while
            # the caller handles this part.
            await start()
            yield part
    finally:
        await folders.close()


async def _next_part(
    parts: asyncio.Queue[_Listing], folders: _Queue[tuple[str, int]]
) -> _Listing | None:
    """The next part listed, or None once a listing ends, or a permit comes, first."""
    getter = asyncio.ensure_future(parts.get())
    try:
        done, _ = await asyncio.wait(
            [getter, *folders.pending()], return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            if task is not getter:
                folders.pop(task)  # A listing's parts are all queued already.
        return getter.result() if getter.done() else None
    finally:
        # A part not got yet stays in the queue: cancelling a get takes none.
        if not getter.done():
            getter.cancel()


async def _produce(
    ils: _Ils,
    folder: str,
    depth: int,
    root: bool,
    parts: asyncio.Queue[_Listing],
    chunk_size: int,
    stopped: Callable[[], bool],
) -> None:
    """List a folder, putting its parts in ``parts`` as they come, its error last."""
    first = True
    chunk: list[dict[str, Any]] = []
    try:
        entries = ils(folder)
        try:
            async for entry in entries:
                if entry["name"].rstrip("/") == folder:
                    # A file lists itself, as fsspec's _ls of a file does. Some
                    # storages list a folder within itself too, e.g. as its
                    # marker object.
                    if entry["type"] != "directory" and root:
                        raise NotADirectoryError(folder)
                    continue

                chunk.append(entry)
                if len(chunk) >= chunk_size:
                    await parts.put(_Listing(folder, depth, chunk, None, first, False))
                    first, chunk = False, []
                    if stopped():
                        return
        finally:
            if (aclose := getattr(entries, "aclose", None)) is not None:
                await aclose()
    except Exception as e:
        await parts.put(_Listing(folder, depth, [], e, first, True))
        return

    await parts.put(_Listing(folder, depth, chunk, None, first, True))


def _new_subfolders(
    part: _Listing,
    descend: Callable[[dict[str, Any]], bool] | None,
    found: dict[str, set[str]],
) -> list[tuple[str, int]]:
    """The subfolders a part adds to list, with their depth, those found before left out.

    Each comes once, though a storage may list a folder twice, e.g. with and
    without its marker object, in two parts.
    """
    seen = found.setdefault(part.folder, set())
    new = [
        s for s in dict.fromkeys(_subfolders(part.entries, descend)) if s not in seen
    ]
    seen.update(new)
    return [(s, part.depth + 1) for s in new]


def _note(error: Exception, folder: str) -> None:
    """Note its folder on a listing's error, yielded among folders.

    In its note for people, and as an attribute for code, e.g. fsspec's walk.
    """
    error.add_note(f"Listing {folder!r} failed.")
    error.folder = folder  # type: ignore[attr-defined]


async def _listings(
    ils: _Ils,
    path: str,
    *,
    maxdepth: int | None = None,
    topdown: bool = True,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    raise_on_error: bool = False,
    concurrency_limit: asyncio.Semaphore,
    stopping: Callable[[], object] | None = None,
) -> AsyncGenerator[_Listing]:
    """Walk a folder tree as `tree_walk` does, yielding each folder's listing whole.

    It gathers the parts of `_chunks` into one listing per folder, as fsspec's
    walk gives a folder at a time, so it holds each folder until it is done.
    A failed listing comes as one, with its error and no entries. A folder
    stopped before its last part is left out.

    Bottom up, each folder waits for its subfolders, which are only all known
    once its listing ends.
    """
    held: dict[str, list[dict[str, Any]]] = {}
    bottom_up = None if topdown else _BottomUp()
    chunks = _chunks(
        ils,
        path,
        maxdepth=maxdepth,
        descend=descend,
        raise_on_error=raise_on_error,
        concurrency_limit=concurrency_limit,
        stopping=stopping,
    )

    async with aclosing(chunks):
        async for part in chunks:
            entries = held.setdefault(part.folder, [])
            entries.extend(part.entries)
            if not part.last:
                continue

            del held[part.folder]
            if part.error is not None:
                listing = _Listing(part.folder, part.depth, [], part.error)
            else:
                listing = _Listing(part.folder, part.depth, entries)

            if bottom_up is None:
                yield listing
                continue

            subfolders = []
            if listing.error is None and (maxdepth is None or listing.depth < maxdepth):
                subfolders = list(dict.fromkeys(_subfolders(listing.entries, descend)))
            for ready in bottom_up.add(listing, subfolders):
                yield ready


def _subfolders(
    entries: list[dict[str, Any]],
    descend: Callable[[dict[str, Any]], bool] | None,
) -> list[str]:
    """The subfolders among entries to list next."""
    return [
        e["name"].rstrip("/")
        for e in entries
        if e["type"] == "directory" and (descend is None or descend(e))
    ]


class _BottomUp:
    """Holds each listing back until the listings of its subfolders are out."""

    def __init__(self) -> None:
        # Each listing held back, with how many of its subfolders are not out.
        self.held: dict[str, tuple[_Listing, int]] = {}
        self.parents: dict[str, str] = {}

    def add(self, listing: _Listing, subfolders: list[str]) -> list[_Listing]:
        """The listings ready once this one is in, deepest first."""
        if subfolders:
            self.held[listing.folder] = listing, len(subfolders)
            self.parents.update(dict.fromkeys(subfolders, listing.folder))
            return []

        ready = [listing]
        folder = listing.folder

        # A subtree done may complete its parent's, and so on up.
        while (parent := self.parents.pop(folder, None)) is not None:
            held, remaining = self.held[parent]
            if remaining > 1:
                self.held[parent] = held, remaining - 1
                break
            del self.held[parent]
            ready.append(held)
            folder = parent

        return ready
