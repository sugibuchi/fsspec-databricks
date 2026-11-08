# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A concurrent walk of a folder tree, a folder per listing request.

`tree_walk` lists folders breadth first, several at a time, and yields each
folder's ``(path, dirs, files)`` as its listing arrives, as fsspec's ``walk``
gives them. A ``descend`` predicate prunes the folders it lists, as glob needs. Its engine, ``_listings``, is also the engine under the
other tree strategies, such as find, rm and du.

It suits storages that list one folder per request, and those whose folders
can be pruned. On object stores, one recursive listing usually costs fewer
requests: `fsspec.asyn.AsyncFileSystem._find` there.

The tree strategies take the storage's operations as functions, such as
``ls``, rather than a filesystem: see `common.types`.

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

    from ..common.types import _Ls

__all__ = ["WalkState", "tree_walk"]


def tree_walk(
    path: str,
    *,
    ls: _Ls,
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
    ``ls`` entries. An empty folder yields empty ones. The folders come in the
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

    Requests: one ``ls`` per folder walked, as ``concurrency_limit`` allows.

    Parameters
    ----------
    path : str
        The folder to start from, as ``ls`` takes it.
    ls : callable
        Lists a folder, called as ``ls(path, detail=True)``: e.g. the
        filesystem's ``_ls``.
    maxdepth : int, optional
        The deepest level to list: 1 lists only ``path``. No limit by default.
    topdown : bool
        Whether a folder comes before those below it, rather than after.
    descend : callable, optional
        Called with the ``ls`` entry of each subfolder found. The walk lists it
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
    >>> walk = tree_walk("data", ls=fs._ls, concurrency_limit=limit)
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
            ls,
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
    """A folder's listing, as ``_listings`` yields it.

    ``depth`` is 1 for the folder the walk started from, 2 for its subfolders,
    and so on. ``entries`` are as ``ls`` gives them, without the folder itself,
    and empty if the listing failed with ``error``.

    ``depth`` 0 is no listing, but the walk's root itself, as `_find_listings`
    yields it: ``entries`` hold only the root's ``info`` entry, under ``folder``.
    """

    folder: str
    depth: int
    entries: list[dict[str, Any]]
    error: Exception | None = None


async def _listings(
    ls: _Ls,
    path: str | Collection[str],
    *,
    maxdepth: int | None = None,
    topdown: bool = True,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    raise_on_error: bool = False,
    concurrency_limit: asyncio.Semaphore,
    stopping: Callable[[], object] | None = None,
) -> AsyncGenerator[_Listing]:
    """Walk a folder tree as `tree_walk` does, yielding each folder's listing.

    The tree strategies need a listing's depth and error, which `tree_walk`'s
    folders do not carry. A failed listing is yielded with its error, and its
    subfolders are not walked. ``path``'s own listing fails by raising, as fsspec's walk
    does, since nothing has been done yet.

    ``path`` may be several folders, e.g. a glob's matches, walked at once, each
    as a root. None may be inside another.

    With ``raise_on_error``, any failed listing raises as soon as it is seen:
    before another listing starts, or another one done is yielded.

    Once ``stopping`` returns true, no new listing starts, and those in flight
    are yielded as they arrive. It is an operation's ``cancelling``.

    Each listing holds a permit of ``concurrency_limit`` while it runs.
    """
    _check_maxdepth(maxdepth)

    roots = {_trim(p) for p in ([path] if isinstance(path, str) else path)}
    # The folders to list, each with its depth.
    folders: _Queue[tuple[str, int]] = _Queue(concurrency_limit)
    bottom_up = None if topdown else _BottomUp()

    async def start() -> None:
        if stopping is not None and stopping():
            folders.waiting.clear()
        await folders.start(lambda item: ls(item[0], detail=True))

    folders.waiting.extend((root, 1) for root in sorted(roots))
    await start()

    try:
        while pending := folders.pending():
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if (item := folders.pop(task)) is None:
                    continue
                folder, depth = item
                listing = _listing(task, folder, depth, roots, raise_on_error)
                subfolders = _subfolders(listing, maxdepth, descend)
                folders.waiting.extend((s, depth + 1) for s in subfolders)

                # Start the next listings before yielding, so that they run
                # while the caller handles this one.
                await start()

                if bottom_up is None:
                    yield listing
                else:
                    for ready in bottom_up.add(listing, subfolders):
                        yield ready

            await start()
    finally:
        await folders.close()


def _listing(
    task: asyncio.Task[list[dict[str, Any]]],
    folder: str,
    depth: int,
    roots: set[str],
    raise_on_error: bool,
) -> _Listing:
    """The listing a finished ``ls`` task gives, or its error."""
    try:
        entries = task.result()
    except Exception as e:
        # A root's error is raised as fsspec's own walk raises it: nothing has
        # been done yet. Raised here, any other stops the walk before it starts
        # another listing.
        if folder in roots or raise_on_error:
            raise

        # Yielded among folders, the error must tell its folder itself: in its
        # note for people, and as an attribute for code, e.g. fsspec's walk.
        e.add_note(f"Listing {folder!r} failed.")
        e.folder = folder  # type: ignore[attr-defined]
        return _Listing(folder, depth, [], e)

    if folder in roots and any(
        e["name"].rstrip("/") == folder and e["type"] != "directory" for e in entries
    ):
        # fsspec's _ls of a file lists the file itself.
        raise NotADirectoryError(folder)

    # Some storages list a folder within itself, e.g. as its marker object.
    return _Listing(
        folder, depth, [e for e in entries if e["name"].rstrip("/") != folder]
    )


def _subfolders(
    listing: _Listing,
    maxdepth: int | None,
    descend: Callable[[dict[str, Any]], bool] | None,
) -> list[str]:
    """The subfolders of a listing to list next."""
    if listing.error is not None or (
        maxdepth is not None and listing.depth >= maxdepth
    ):
        return []

    # Each once, though a storage may list a folder twice, e.g. with and without
    # its marker object: bottom up counts on it.
    return list(
        dict.fromkeys(
            e["name"].rstrip("/")
            for e in listing.entries
            if e["type"] == "directory" and (descend is None or descend(e))
        )
    )


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
