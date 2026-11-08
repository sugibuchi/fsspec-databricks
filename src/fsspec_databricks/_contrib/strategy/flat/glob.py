# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A glob that lists only the keys starting as its pattern does.

`flat_glob` is an alternative to fsspec's ``_glob`` for flat storages, with the
same matches. fsspec's finds every path under the pattern's literal folder,
then filters them. A flat storage lists by any prefix, not only by folder: so
`flat_glob` lists only the keys starting with the pattern's text before its
first wildcard, e.g. ``data/2024-`` for ``data/2024-*.parquet``.

The paths are matched as `tree_glob` matches them, with the same compiled
patterns: the two never answer differently.

This module depends on `_keys`, and `operation`, `paths` and `patterns` of
`common`.
"""

from __future__ import annotations

from glob import has_magic
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import GlobState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from ..common.patterns import _infos, _Search
from ._keys import _close, _folder, _folders, _new

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncGenerator, Sequence

    from ..common.types import _IExpand, _Info

__all__ = ["GlobState", "flat_glob"]


def flat_glob(
    path: str | Sequence[str],
    *,
    iexpand: _IExpand,
    info: _Info,
    maxdepth: int | None = None,
    withdirs: bool = True,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[dict[str, Any], GlobState]:
    """Find the paths matching glob patterns, listing only the keys they can match.

    It matches what fsspec's ``_glob`` matches, with ``*``, ``?``, ``[...]`` and
    ``**``, filtered with fsspec's own regular expression. The folders are
    derived from the keys, as fsspec's filesystems derive them for object
    stores, and a key ending with "/" is a folder's marker. A derived folder's
    entry has only its ``name``, ``size`` 0 and ``type``.

    Each pattern lists the keys starting with its text before the first
    wildcard. It yields each match as its page of the listing arrives, so the
    matches are not in order: fsspec's ``_glob`` sorts them, by name.

    Several patterns match what fsspec's globs of each match together, each
    path once. A pattern whose prefix starts with another's is matched in that
    one's listing. The other listings run one after another.

    It wins over fsspec's ``_glob`` on flat storages when the text before the
    first wildcard ends within a name, e.g. ``data/2024-*``: fsspec's lists all
    of ``data``. It wins too when the caller starts on the first matches, or
    the listing is large. It loses on storages with a hierarchical namespace,
    whose empty folders a listing by prefix does not see, and which `tree_glob`
    prunes by folder.

    Once cancelled, it takes nothing more from the listings. An error from a
    listing ends the operation, raised by its iteration.

    Requests: the pages of one ``iexpand`` per listing, one after another. One
    ``info`` per pattern without wildcards.

    Parameters
    ----------
    path : str or sequence of str
        The pattern, or patterns, of paths as ``iexpand`` takes them. A trailing
        "/" matches only folders.
    iexpand : callable
        Lists every key starting with a prefix, called as ``iexpand(prefix)``: an
        async iterator of entries as fsspec's ``_ls`` gives them, in order of
        their names, each key once, as an object store lists them.
    info : callable
        Describes a path, called as ``info(path)``: e.g. the filesystem's
        ``_info``.
    maxdepth : int, optional
        How deep a ``**`` goes, as with fsspec's ``_glob``. No limit by
        default. The listing still lists every key under the prefix.
    withdirs : bool
        Whether folders can match, as with fsspec's ``_glob``.
    concurrency_limit : asyncio.Semaphore
        Held by each ``info`` while it runs. The listings hold none.

    Returns
    -------
    Operation
        Yielding the entry of each path matched. Its state is a
        `GlobState`.

    Raises
    ------
    ValueError
        If ``maxdepth`` is less than 1, or a ``**`` is not a whole segment.

    See Also
    --------
    tree_glob : The same, for storages listing one folder per request.

    Examples
    --------
    >>> limit = asyncio.Semaphore(32)  # Shared, e.g. by a filesystem's operations.
    >>> glob = flat_glob(
    ...     "data/2024-*.parquet", iexpand=my_ils, info=fs._info, concurrency_limit=limit
    ... )
    >>> async with glob as matches:
    ...     async for match in matches:
    ...         print(match["name"])
    data/2024-01.parquet
    """
    _check_maxdepth(maxdepth)

    literals: list[str] = []
    searches: list[tuple[str, _Search]] = []
    for pattern in [path] if isinstance(path, str) else path:
        trimmed = _trim(pattern)
        if has_magic(trimmed):
            first_magic = min(i for i in (trimmed.find(c) for c in "*?[") if i >= 0)
            search = _Search(trimmed, pattern.endswith("/"), withdirs, maxdepth)
            searches.append((trimmed[:first_magic], search))
        else:
            literals.append(trimmed)

    async def run(
        op: Operation[dict[str, Any], GlobState],
    ) -> AsyncGenerator[dict[str, Any]]:
        found: set[str] = set()
        async for entry in _infos(info, literals, concurrency_limit, op, found):
            yield entry

        for prefix, group in _listings(searches):
            if op.cancelling():
                return
            async for match in _matches(iexpand, prefix, group, op, found):
                yield match

    return Operation(run, GlobState())


def _listings(
    searches: list[tuple[str, _Search]],
) -> list[tuple[str, list[_Search]]]:
    """The prefixes to list, each with the patterns it serves.

    A pattern whose prefix starts with another's is matched in that one's
    listing: it lists all the keys the pattern can match.
    """
    prefixes = sorted({p for p, _ in searches})
    tops = [p for p in prefixes if not any(p.startswith(o) for o in prefixes if o != p)]
    return [(top, [s for p, s in searches if p.startswith(top)]) for top in tops]


async def _matches(
    iexpand: _IExpand,
    prefix: str,
    searches: list[_Search],
    op: Operation[dict[str, Any], GlobState],
    found: set[str],
) -> AsyncGenerator[dict[str, Any]]:
    """The matches of patterns in one listing, noted in ``found``."""
    # The folders of the key listed last.
    before: list[str] = []

    entries = iexpand(prefix)
    try:
        async for entry in entries:
            name = entry["name"]

            # The folders the key implies, each once, then the key itself.
            folders = _folders(name)
            candidates = [_folder(f) for f in _new(folders, before)]
            before = folders
            if not name.endswith("/"):
                candidates.append(entry)

            for candidate in candidates:
                matched = candidate["name"]
                if matched in found or not any(s.matched(candidate) for s in searches):
                    continue
                found.add(matched)
                op.state = GlobState(
                    errors=op.state.errors,
                    current=matched,
                    matched=op.state.matched + 1,
                )
                yield candidate

            if op.cancelling():
                return
    finally:
        await _close(entries)
