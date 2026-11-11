# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive find that streams one listing of a prefix.

`_flat_find` is an alternative to fsspec's ``_find`` for flat storages, with the
same paths. fsspec's, and the object stores' own, gather every page of the
listing before answering. `_flat_find` yields each path as its page arrives.

This module depends on `_keys`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import FindState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from ._keys import _close, _dir, _dirs, _listing, _new

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

__all__: list[str] = []


def _flat_find(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    withdirs: bool = False,
) -> Operation[dict[str, Any], FindState]:
    """Find the files under a prefix, from one listing streamed as it comes.

    It finds the paths fsspec's ``_find`` finds. A missing ``path`` finds
    nothing, and a file finds itself. With ``withdirs``, the directories are
    derived from the keys, as fsspec's filesystems derive them for object
    stores, and a key ending with "/" is a directory's marker. A derived directory's
    entry has only its ``name``, ``size`` 0 and ``type``.

    It yields each path as its page of the listing arrives, so the first come
    after one request, not after the last. They come in the listing's order,
    with each directory before its first key. fsspec's ``_find`` sorts them, by
    name.

    It wins over fsspec's ``_find`` on large prefixes of flat storages: the
    caller starts on the first page, and holds one page at a time, not the
    whole listing. It sends the same listing requests. It loses on storages
    with a hierarchical namespace, whose directories a listing by prefix does not
    see when empty: `_tree_find` suits those.

    Once cancelled, it takes nothing more from the listing. An error from the
    listing ends the operation, raised by its iteration.

    Requests: the pages of one recursive ``_ils`` of ``path``, one after
    another. One ``_info`` for ``path`` itself, if nothing is under it, or
    with ``withdirs``, holding a permit of the filesystem's
    ``operation_limit``. The listing holds none.

    ``path`` is the directory searched on ``fs``. Its recursive ``_ils``
    lists every key below a directory, in name order, each key once. ``maxdepth`` of
    1 finds only the files in ``path``, though the listing still lists every
    key under it. ``withdirs`` finds directories too, ``path`` included.

    The operation yields the entry of each path found, and its state is a
    `FindState`. The call raises ValueError if ``maxdepth`` is less than 1.
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], FindState],
    ) -> AsyncGenerator[dict[str, Any]]:
        steps = _find_entries(
            fs,
            path,
            maxdepth=maxdepth,
            withdirs=withdirs,
            stopping=op.cancelling,
        )

        async with aclosing(steps):
            async for entry in steps:
                op.state = FindState(current=entry["name"], found=op.state.found + 1)
                yield entry

    return Operation(run, FindState())


async def _find_entries(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    withdirs: bool = False,
    stopping: Callable[[], object] | None = None,
) -> AsyncGenerator[dict[str, Any]]:
    """The entries under ``path``, as fsspec's ``_find`` finds them.

    It yields each file under ``path`` as the listing gives it, and with
    ``withdirs`` each directory once, derived from the keys. ``path`` itself is
    treated as fsspec's ``_find`` treats it:

    - missing, it yields nothing;
    - a file, it yields the file's ``_info`` entry;
    - a directory, it yields the directory's own ``_info`` entry with ``withdirs``,
      unless stopping.

    Once ``stopping`` returns true, it takes no more from the listing. An error
    of the listing ends it, as it ends the only listing there is.
    """
    root = _trim(path)
    prefix = f"{root}/" if root else ""
    under = False

    # The directories of the key listed last.
    before: list[str] = []

    entries = _listing(fs, prefix)
    try:
        async for entry in entries:
            under = True
            name = entry["name"]
            if withdirs:
                directories = _dirs(name[len(prefix) :])
                for directory in _new(directories, before):
                    if _within(directory, maxdepth):
                        yield _dir(prefix + directory)
                before = directories
            if not name.endswith("/") and _within(name[len(prefix) :], maxdepth):
                yield entry
            if stopping is not None and stopping():
                return
    finally:
        await _close(entries)

    if not root or (under and not withdirs):
        return

    try:
        async with fs.operation_limit:
            entry = await fs._info(root)
    except FileNotFoundError:
        return

    # A directory is found itself only with withdirs. A file is found itself only
    # when nothing is under it, as fsspec's _find tells them apart.
    is_dir = entry["type"] == "directory"
    if (is_dir and withdirs) or (not is_dir and not under):
        yield entry


def _within(relative: str, maxdepth: int | None) -> bool:
    """Whether a path, relative to the root, is at most ``maxdepth`` deep."""
    return maxdepth is None or relative.count("/") + 1 <= maxdepth
