# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The size of the files under a prefix, from one listing streamed as it comes.

`flat_du` is an alternative to fsspec's ``_du`` for flat storages. fsspec's
finds the files, then asks each file's size with a request of its own. `flat_du`
adds up the sizes the listing already gives, page by page.

This module depends on `find`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import DuState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth
from .find import _find_entries

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator

    from ..common.types import _Ils, _Info

__all__ = ["DuState", "flat_du"]


def flat_du(
    path: str,
    *,
    ils: _Ils,
    info: _Info,
    maxdepth: int | None = None,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[dict[str, Any], DuState]:
    """Measure the files under a prefix, from one listing streamed as it comes.

    It assumes that ``ils`` gives each file's size, as listings of object
    stores do. It yields the entry of each file counted, with its ``size``, as
    fsspec's ``_du`` gives with ``total=False``. Its state's ``size`` is their
    total, as fsspec's ``_du`` gives by default.

    A missing ``path`` measures nothing, as with fsspec's ``_du``. A file
    measures itself. A key ending with "/" is a folder's marker, and not
    counted.

    It wins over fsspec's ``_du`` on any flat storage, as it sends no ``info``
    per file, and holds one page of the listing at a time.

    Once cancelled, it takes nothing more from the listing. An error from the
    listing ends the operation, raised by its iteration.

    Requests: the pages of one ``ils`` of ``path``, one after another. One
    ``info`` for ``path``, if nothing is under it.

    Parameters
    ----------
    path : str
        The folder to measure, as ``ils`` takes paths.
    ils : callable
        Lists every key starting with a prefix, called as ``ils(prefix)``: an
        async iterator of entries as fsspec's ``_ls`` gives them, in order of
        their names, each key once, as an object store lists them.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    maxdepth : int, optional
        The deepest level to measure: 1 counts only the files in ``path``. No
        limit by default. The listing still lists every key under it.
    concurrency_limit : asyncio.Semaphore
        Held by the ``info`` while it runs. The listing holds none.

    Returns
    -------
    Operation
        Yielding the entry of each file counted. Its state is a `DuState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1.

    See Also
    --------
    tree_du : The same, for storages listing one folder per request.

    Examples
    --------
    >>> limit = asyncio.Semaphore(32)  # Shared, e.g. by a filesystem's operations.
    >>> op = flat_du("data", ils=my_ils, info=fs._info, concurrency_limit=limit)
    >>> async for _ in op:
    ...     pass
    >>> op.state.size, op.state.files
    (73400320, 70)
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], DuState],
    ) -> AsyncGenerator[dict[str, Any]]:
        steps = _find_entries(
            ils,
            info,
            path,
            maxdepth=maxdepth,
            concurrency_limit=concurrency_limit,
            stopping=op.cancelling,
        )

        async with aclosing(steps):
            async for entry in steps:
                op.state = DuState(
                    current=entry["name"],
                    files=op.state.files + 1,
                    size=op.state.size + (entry.get("size") or 0),
                )
                yield entry

    return Operation(run, DuState())
