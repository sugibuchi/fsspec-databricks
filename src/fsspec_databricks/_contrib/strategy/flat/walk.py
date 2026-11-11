# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A walk of the keys under a prefix, from one listing streamed as it comes.

`_flat_walk` is the counterpart of `_tree_walk` for flat storages. fsspec's
``walk`` lists one directory per request, even on object stores, which can list
all the keys under a prefix at once. `_flat_walk` makes that one listing, and
derives the directories from the keys. As the keys come in order, those in a
directory come together: the directory is done once the listing has passed them.

This module depends on `_keys`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import WalkState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim, _triples
from ._keys import _close, _dir, _dirs, _listing

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

__all__: list[str] = []


def _flat_walk(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    topdown: bool = True,
    descend: Callable[[dict[str, Any]], bool] | None = None,
) -> Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState]:
    """Walk the keys under a prefix, from one listing streamed as it comes.

    It yields each directory as ``(path, dirs, files)``, as `_tree_walk` does:
    ``path`` itself, then each directory below it. ``dirs`` and ``files`` map the
    names in the directory to their entries. The files are the keys, as the
    recursive ``_ils`` gives them. The directories are derived from the keys, as fsspec's
    filesystems derive them for object stores, and a key ending with "/" is a
    directory's marker. A derived directory's entry has only its ``name``, ``size`` 0
    and ``type``.

    A directory is done once the listing has passed its keys. Without
    ``topdown``, each directory comes as soon as it is done, after all those
    below it, as fsspec's ``walk`` orders them. With ``topdown``, a directory
    comes before those below it, but ``path`` is done only once the listing
    ends: so all of them come then. A walk that is cancelled leaves out the
    directories not done.

    ``maxdepth`` and ``descend`` leave directories out, but save no request: the
    listing gives all the keys under ``path`` anyway.

    It wins over fsspec's ``walk`` on flat storages, with many directories: one
    listing, of as many pages as there are keys, rather than a listing per
    directory. It loses on storages with a hierarchical namespace, whose empty
    directories a listing by prefix does not see: `_tree_walk` suits those.

    Once cancelled, it takes nothing more from the listing. Iterating it raises
    these errors, which end it:

    - FileNotFoundError, if ``path`` does not exist;
    - NotADirectoryError, if ``path`` is a file;
    - the error of the listing, as there is no other to go on with.

    Requests: the pages of one recursive ``_ils`` of ``path``, one after
    another. One ``_info`` for ``path``, if nothing is under it, holding a
    permit of the filesystem's ``operation_limit``. The listing holds none.

    ``path`` is the directory the walk starts from on ``fs``. Its recursive
    ``_ils`` lists every key below a directory, in name order, each key once,
    as an object store lists them. ``maxdepth`` of 1 yields only ``path``.
    ``topdown`` puts a directory before those below it. ``descend``, called with
    the entry of each subdirectory found, lets the walk enter it only if it
    returns True.

    The operation yields ``(path, dirs, files)`` for each directory walked, and
    its state is a `WalkState`. The call raises ValueError if ``maxdepth`` is
    less than 1.
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[tuple[str, dict[str, Any], dict[str, Any]], WalkState],
    ) -> AsyncGenerator[tuple[str, dict[str, Any], dict[str, Any]]]:
        directories = _dirs_done(
            fs,
            path,
            maxdepth=maxdepth,
            descend=descend,
            stopping=op.cancelling,
        )

        # Top down, the directories done, held until the listing ends.
        held: list[tuple[str, list[dict[str, Any]]]] = []

        async with aclosing(directories):
            async for directory, entries in directories:
                if topdown:
                    held.append((directory, entries))
                    continue

                op.state = WalkState(
                    current=directory, found=op.state.found + len(entries)
                )
                yield _triples(directory, entries, detail=True)

        # By name, so that each directory comes before those below it.
        for directory, entries in sorted(held):
            op.state = WalkState(current=directory, found=op.state.found + len(entries))
            yield _triples(directory, entries, detail=True)

    return Operation(run, WalkState())


async def _dirs_done(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None,
    descend: Callable[[dict[str, Any]], bool] | None,
    stopping: Callable[[], object],
) -> AsyncGenerator[tuple[str, list[dict[str, Any]]]]:
    """Each directory walked with its entries, as soon as it is done: bottom up.

    The directories open, those the listing is in, are ``path`` and its subdirectories
    down to the key listed last. A key not under the deepest one closes it: the
    keys come in order, so none under it comes later. A walk stopped leaves
    out the directories still open.
    """
    root = _trim(path)
    prefix = f"{root}/" if root else ""
    under = False

    # The directories open, the root first, each with its entries so far.
    open_: list[tuple[str, list[dict[str, Any]]]] = [(root, [])]

    keys = _listing(fs, prefix)
    try:
        async for key in keys:
            under = True
            name = key["name"]
            while len(open_) > 1 and not name.startswith(open_[-1][0] + "/"):
                yield open_.pop()

            _add(key, name[len(prefix) :], prefix, open_, maxdepth, descend)
            if stopping():
                return
    finally:
        await _close(keys)

    if not under and root:
        # Nothing under it: a file, or missing, as _tree_walk raises for them.
        async with fs.operation_limit:
            entry = await fs._info(root)
        if entry["type"] != "directory":
            raise NotADirectoryError(root)

    while open_:
        yield open_.pop()


def _add(
    key: dict[str, Any],
    relative: str,
    prefix: str,
    open_: list[tuple[str, list[dict[str, Any]]]],
    maxdepth: int | None,
    descend: Callable[[dict[str, Any]], bool] | None,
) -> None:
    """Add a key to the walk: the directories it implies first seen, then itself.

    The directories already open are the key's parents. Each new one goes into
    its parent's entries, and is opened unless ``maxdepth`` or ``descend``
    leaves out what is in it. A key in a directory left out adds nothing more.
    """
    if not relative:
        return  # The root's own marker.

    for directory in _dirs(relative)[len(open_) - 1 :]:
        full = prefix + directory
        entries = open_[-1][1]
        last = entries[-1] if entries else None
        if last is not None and last["name"] == full and last["type"] == "directory":
            return  # In a directory left out, seen with a key before.

        entry = _dir(full)
        entries.append(entry)

        depth = directory.count("/") + 1
        if (maxdepth is not None and depth >= maxdepth) or (
            descend is not None and not descend(entry)
        ):
            return

        open_.append((full, []))

    if not relative.endswith("/"):
        open_[-1][1].append(key)
