# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""The size of a tree, from its listings alone.

`_tree_du` is an alternative to fsspec's ``_du``. fsspec's finds the files, then asks
each file's size with a request of its own. `_tree_du` adds up the sizes the
listings already give.

This module depends on `find`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import DuState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth
from .find import _find_listings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


__all__: list[str] = []


def _tree_du(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    ils_kwargs: dict[str, Any] | None = None,
) -> Operation[dict[str, Any], DuState]:
    """Measure a tree's size from its listings, listing several directories at a time.

    It assumes that ``_ils`` gives each file's size, as fsspec's filesystems do.
    It yields the entry of each file counted, with its ``size``, as fsspec's
    ``_du`` gives with ``total=False``. Its state's ``size`` is their total, as
    fsspec's ``_du`` gives by default.

    A missing ``path`` measures nothing, as with fsspec's ``_du``. A file
    measures itself. A directory that fails to list yields its error, and the rest
    of the tree is still measured.

    It wins over fsspec's ``_du`` on any storage, as it sends no ``info`` per
    file. On object stores, which list a whole tree in one paginated request,
    summing the sizes from fsspec's ``_find`` with ``detail=True`` makes fewer
    requests still.

    Once cancelled, it lists no new directory, and counts the listings in flight.

    Requests: one recursive ``_ils`` of ``path``, which a storage without a
    recursive listing answers with one per directory. With ``maxdepth``, one
    ``_ils`` per directory instead, as the filesystem's ``operation_limit``
    allows. One ``info`` for ``path``, if a file.

    ``path`` is the directory measured on ``fs``, by its ``_ils`` and ``_info``.
    ``maxdepth`` of 1 counts only the files in ``path``. ``ils_kwargs`` go to
    each ``_ils``, as fsspec's ``du`` passes its own on.

    The operation yields the ``_ils`` entry of each file counted, or the error
    of a directory that failed to list, and its state is a `DuState`. The call
    raises ValueError if ``maxdepth`` is less than 1.
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], DuState],
    ) -> AsyncGenerator[dict[str, Any] | Exception]:
        steps = _find_listings(
            fs,
            path,
            maxdepth=maxdepth,
            ils_kwargs=ils_kwargs,
            stopping=op.cancelling,
            file_root=True,
        )
        state = op.state

        async with aclosing(steps):
            async for listing in steps:
                files = [e for e in listing.entries if e["type"] != "directory"]
                op.state = state = DuState(
                    errors=state.errors + (listing.error is not None),
                    current=listing.directory,
                    # Depth 0 is the root's own entry: no directory listed.
                    directories_listed=state.directories_listed
                    + (listing.depth > 0 and listing.last),
                    files=state.files + len(files),
                    size=state.size + sum(e.get("size") or 0 for e in files),
                )

                if listing.error is not None:
                    yield listing.error

                for entry in files:
                    yield entry

    return Operation(run, DuState(directories_listed=0))
