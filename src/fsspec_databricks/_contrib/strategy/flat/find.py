# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive find that streams one listing of a prefix.

`flat_find` is an alternative to fsspec's ``_find`` for flat storages, with the
same paths. fsspec's, and the object stores' own, gather every page of the
listing before answering. `flat_find` yields each path as its page arrives.

This module depends on `_keys`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import FindState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from ._keys import _close, _folder, _folders, _new

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, Callable

    from ..common.types import _IExpand, _Info

__all__ = ["FindState", "flat_find"]


def flat_find(
    path: str,
    *,
    iexpand: _IExpand,
    info: _Info,
    maxdepth: int | None = None,
    withdirs: bool = False,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[dict[str, Any], FindState]:
    """Find the files under a prefix, from one listing streamed as it comes.

    It finds the paths fsspec's ``_find`` finds. A missing ``path`` finds
    nothing, and a file finds itself. With ``withdirs``, the folders are
    derived from the keys, as fsspec's filesystems derive them for object
    stores, and a key ending with "/" is a folder's marker. A derived folder's
    entry has only its ``name``, ``size`` 0 and ``type``.

    It yields each path as its page of the listing arrives, so the first come
    after one request, not after the last. They come in the listing's order,
    with each folder before its first key. fsspec's ``_find`` sorts them, by
    name.

    It wins over fsspec's ``_find`` on large prefixes of flat storages: the
    caller starts on the first page, and holds one page at a time, not the
    whole listing. It sends the same listing requests. It loses on storages
    with a hierarchical namespace, whose folders a listing by prefix does not
    see when empty: `tree_find` suits those.

    Once cancelled, it takes nothing more from the listing. An error from the
    listing ends the operation, raised by its iteration.

    Requests: the pages of one ``iexpand`` of ``path``, one after another. One
    ``info`` for ``path`` itself, if nothing is under it, or with ``withdirs``.

    Parameters
    ----------
    path : str
        The folder to search, as ``iexpand`` takes paths.
    iexpand : callable
        Lists every key starting with a prefix, called as ``iexpand(prefix)``: an
        async iterator of entries as fsspec's ``_ls`` gives them, in order of
        their names, each key once, as an object store lists them.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    maxdepth : int, optional
        The deepest level to find: 1 finds only the files in ``path``. No
        limit by default. The listing still lists every key under it.
    withdirs : bool
        Whether to find folders too, ``path`` included.
    concurrency_limit : asyncio.Semaphore
        Held by each ``info`` while it runs. The listing holds none.

    Returns
    -------
    Operation
        Yielding the entry of each path found. Its state is a `FindState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1.

    See Also
    --------
    tree_find : The same, for storages listing one folder per request.

    Examples
    --------
    >>> limit = asyncio.Semaphore(32)  # Shared, e.g. by a filesystem's operations.
    >>> find = flat_find(
    ...     "data", iexpand=fs._iexpand, info=fs._info, concurrency_limit=limit
    ... )
    >>> async with find as op:
    ...     async for entry in op:
    ...         print(entry["name"])
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], FindState],
    ) -> AsyncGenerator[dict[str, Any]]:
        steps = _find_entries(
            iexpand,
            info,
            path,
            maxdepth=maxdepth,
            withdirs=withdirs,
            concurrency_limit=concurrency_limit,
            stopping=op.cancelling,
        )

        async with aclosing(steps):
            async for entry in steps:
                op.state = FindState(current=entry["name"], found=op.state.found + 1)
                yield entry

    return Operation(run, FindState())


async def _find_entries(
    iexpand: _IExpand,
    info: _Info,
    path: str,
    *,
    maxdepth: int | None = None,
    withdirs: bool = False,
    concurrency_limit: asyncio.Semaphore,
    stopping: Callable[[], object] | None = None,
) -> AsyncGenerator[dict[str, Any]]:
    """The entries under ``path``, as fsspec's ``_find`` finds them.

    It yields each file under ``path`` as the listing gives it, and with
    ``withdirs`` each folder once, derived from the keys. ``path`` itself is
    treated as fsspec's ``_find`` treats it:

    - missing, it yields nothing;
    - a file, it yields the file's ``info`` entry;
    - a folder, it yields the folder's own ``info`` entry with ``withdirs``,
      unless stopping.

    Once ``stopping`` returns true, it takes no more from the listing. An error
    of the listing ends it, as it ends the only listing there is.
    """
    root = _trim(path)
    prefix = f"{root}/" if root else ""
    under = False

    # The folders of the key listed last.
    before: list[str] = []

    entries = iexpand(prefix)
    try:
        async for entry in entries:
            under = True
            name = entry["name"]
            if withdirs:
                folders = _folders(name[len(prefix) :])
                for folder in _new(folders, before):
                    if _within(folder, maxdepth):
                        yield _folder(prefix + folder)
                before = folders
            if not name.endswith("/") and _within(name[len(prefix) :], maxdepth):
                yield entry
            if stopping is not None and stopping():
                return
    finally:
        await _close(entries)

    if not root or (under and not withdirs):
        return

    try:
        async with concurrency_limit:
            entry = await info(root)
    except FileNotFoundError:
        return

    # A folder is found itself only with withdirs. A file is found itself only
    # when nothing is under it, as fsspec's _find tells them apart.
    is_folder = entry["type"] == "directory"
    if (is_folder and withdirs) or (not is_folder and not under):
        yield entry


def _within(relative: str, maxdepth: int | None) -> bool:
    """Whether a path, relative to the root, is at most ``maxdepth`` deep."""
    return maxdepth is None or relative.count("/") + 1 <= maxdepth
