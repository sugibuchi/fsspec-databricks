# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive find that lists several folders at a time.

`tree_find` is an alternative to fsspec's ``_find``, with the same paths. fsspec's
walks a tree one folder at a time, unless the filesystem lists whole trees.
`tree_find` lists several folders at a time, as `tree_walk` does.

This module depends on `walk`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies_common.states import FindState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from .walk import _Listing, _listings

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, Callable

    from ..common.types import _Info, _Ls

__all__ = ["FindState", "tree_find"]


def tree_find(
    path: str,
    *,
    ls: _Ls,
    info: _Info,
    maxdepth: int | None = None,
    withdirs: bool = False,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[dict[str, Any], FindState]:
    """Find the files in a tree, listing several folders at a time.

    It finds the paths fsspec's ``_find`` finds. A missing ``path`` finds
    nothing, and a file finds itself. A folder that fails to list yields its
    error, and the rest of the tree is still searched.

    It yields each path as its folder's listing arrives, so the paths are not
    in order. fsspec's ``_find`` sorts them, by name. A storage listing a
    folder twice, e.g. as a prefix and as its marker object, yields it twice.

    It wins over fsspec's ``_find`` on storages that list one folder per request,
    with many folders: their listings overlap. It loses on object stores, which
    list a whole tree in one paginated request.

    Once cancelled, it lists no new folder, and yields the paths of the
    listings in flight.

    Requests: one ``ls`` per folder, as ``concurrency_limit`` allows. One
    ``info`` more for ``path`` itself, if a file, or a folder with
    ``withdirs``.

    Parameters
    ----------
    path : str
        The folder to search, as ``ls`` takes it.
    ls : callable
        Lists a folder, called as ``ls(path, detail=True)``: e.g. the
        filesystem's ``_ls``.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    maxdepth : int, optional
        The deepest level to search: 1 finds only the files in ``path``. No
        limit by default.
    withdirs : bool
        Whether to find folders too, ``path`` included.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs. Share one to limit several
        operations, or files, together.

    Returns
    -------
    Operation
        Yielding the ``ls`` entry of each path found, or the error of a folder
        that failed to list. Its state is a `FindState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1.

    See Also
    --------
    tree_walk : The walk under it.

    Examples
    --------
    >>> limit = asyncio.Semaphore(32)  # Shared, e.g. by a filesystem's operations.
    >>> find = tree_find("data", ls=fs._ls, info=fs._info, concurrency_limit=limit)
    >>> found = [item async for item in find]
    >>> sorted(e["name"] for e in found if not isinstance(e, Exception))
    ['data/2024/01/part-0.parquet', 'data/2024/02/part-0.parquet']
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], FindState],
    ) -> AsyncGenerator[dict[str, Any] | Exception]:
        root = _trim(path)
        steps = _find_listings(
            ls,
            info,
            root,
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
            stopping=op.cancelling,
            file_root=True,
            folder_root=withdirs and bool(root),
        )
        state = op.state

        async with aclosing(steps):
            async for listing in steps:
                found = [
                    e for e in listing.entries if withdirs or e["type"] != "directory"
                ]
                op.state = state = FindState(
                    errors=state.errors + (listing.error is not None),
                    current=listing.folder,
                    # Depth 0 is the root's own entry: no folder listed.
                    folders_listed=state.folders_listed + (listing.depth > 0),
                    found=state.found + len(found),
                )

                if listing.error is not None:
                    yield listing.error

                for entry in found:
                    yield entry

    return Operation(run, FindState(folders_listed=0))


async def _find_listings(
    ls: _Ls,
    info: _Info,
    path: str,
    *,
    maxdepth: int | None = None,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    concurrency_limit: asyncio.Semaphore,
    stopping: Callable[[], object] | None = None,
    file_root: bool = False,
    folder_root: bool = False,
) -> AsyncGenerator[_Listing]:
    """Walk as the strategies that only read do, such as find, du and glob.

    It yields each folder's listing, as ``_listings`` does, but treats ``path``
    itself as fsspec's ``_find`` and ``_glob`` do:

    - missing, it yields nothing;
    - a file, it yields the file's ``info`` entry if ``file_root``, else
      nothing;
    - a folder, it yields the folder's own ``info`` entry after its listing,
      if ``folder_root`` and the walk is not stopping.

    The root's own entry comes as a `_Listing` of depth 0, which is no listing.
    So the strategies filter and count it as any entry, but count no folder
    listed for it.
    """
    root = _trim(path)
    listings = _listings(
        ls,
        root,
        maxdepth=maxdepth,
        descend=descend,
        concurrency_limit=concurrency_limit,
        stopping=stopping,
    )

    async with aclosing(listings):
        try:
            root_listing = await anext(listings)
        except StopAsyncIteration:
            return  # Stopping before the root was listed.
        except (FileNotFoundError, NotADirectoryError) as e:
            # Only the root's own listing raises them: any later one is yielded.
            if file_root and isinstance(e, NotADirectoryError):
                async with concurrency_limit:
                    entry = await info(root)
                yield _Listing(root, 0, [entry])
            return

        yield root_listing

        if folder_root and not (stopping and stopping()):
            async with concurrency_limit:
                entry = await info(root)
            yield _Listing(root, 0, [entry])

        async for listing in listings:
            yield listing
