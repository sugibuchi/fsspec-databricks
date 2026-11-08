# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The size of a tree, from its listings alone.

`tree_du` is an alternative to fsspec's ``_du``. fsspec's finds the files, then asks
each file's size with a request of its own. `tree_du` adds up the sizes the
listings already give.

This module depends on `find`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies_common.states import DuState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth
from .find import _find_listings

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator

    from ..common.types import _Info, _Ls

__all__ = ["DuState", "tree_du"]


def tree_du(
    path: str,
    *,
    ls: _Ls,
    info: _Info,
    maxdepth: int | None = None,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[dict[str, Any], DuState]:
    """Measure a tree's size from its listings, listing several folders at a time.

    It assumes that ``ls`` gives each file's size, as fsspec's filesystems do.
    It yields the entry of each file counted, with its ``size``, as fsspec's
    ``_du`` gives with ``total=False``. Its state's ``size`` is their total, as
    fsspec's ``_du`` gives by default.

    A missing ``path`` measures nothing, as with fsspec's ``_du``. A file
    measures itself. A folder that fails to list yields its error, and the rest
    of the tree is still measured.

    It wins over fsspec's ``_du`` on any storage, as it sends no ``info`` per
    file. On object stores, which list a whole tree in one paginated request,
    summing the sizes from fsspec's ``_find`` with ``detail=True`` makes fewer
    requests still.

    Once cancelled, it lists no new folder, and counts the listings in flight.

    Requests: one ``ls`` per folder, as ``concurrency_limit`` allows. One
    ``info`` for ``path``, if a file.

    Parameters
    ----------
    path : str
        The folder to measure, as ``ls`` takes it.
    ls : callable
        Lists a folder, called as ``ls(path, detail=True)``: e.g. the
        filesystem's ``_ls``.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    maxdepth : int, optional
        The deepest level to measure: 1 counts only the files in ``path``. No
        limit by default.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs. Share one to limit several
        operations, or files, together.

    Returns
    -------
    Operation
        Yielding the ``ls`` entry of each file counted, or the error of a
        folder that failed to list. Its state is a `DuState`.

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
    >>> du = tree_du("data", ls=fs._ls, info=fs._info, concurrency_limit=limit)
    >>> async with du as op:
    ...     async for entry in op:
    ...         if isinstance(entry, Exception):
    ...             raise entry
    >>> op.state.size, op.state.files
    (73400320, 70)
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], DuState],
    ) -> AsyncGenerator[dict[str, Any] | Exception]:
        steps = _find_listings(
            ls,
            info,
            path,
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
            stopping=op.cancelling,
            file_root=True,
        )
        state = op.state

        async with aclosing(steps):
            async for listing in steps:
                files = [e for e in listing.entries if e["type"] != "directory"]
                op.state = state = DuState(
                    errors=state.errors + (listing.error is not None),
                    current=listing.folder,
                    # Depth 0 is the root's own entry: no folder listed.
                    folders_listed=state.folders_listed + (listing.depth > 0),
                    files=state.files + len(files),
                    size=state.size + sum(e.get("size") or 0 for e in files),
                )

                if listing.error is not None:
                    yield listing.error

                for entry in files:
                    yield entry

    return Operation(run, DuState(folders_listed=0))
