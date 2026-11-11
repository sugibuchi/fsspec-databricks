# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive find that lists several directories at a time.

`_tree_find` is an alternative to fsspec's ``_find``, with the same paths.
fsspec's walks a tree one directory at a time, unless the filesystem lists
whole trees. `_tree_find` lists the tree as `_tree_walk` does: by one recursive
``_ils``, or a directory per request, several at a time.

This module depends on `walk`, and `operation` and `paths` of `common`.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies.states import FindState

from ..common.operation import Operation
from ..common.paths import _check_maxdepth, _trim
from .walk import _chunks, _Listing

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

__all__: list[str] = []


def _tree_find(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    withdirs: bool = False,
    ils_kwargs: dict[str, Any] | None = None,
) -> Operation[dict[str, Any], FindState]:
    """Find the files in a tree, listing several directories at a time.

    It finds the paths fsspec's ``_find`` finds. A missing ``path`` finds
    nothing, and a file finds itself. A directory that fails to list yields its
    error. With ``maxdepth``, the rest of the tree is still searched. Without,
    the tree's one recursive listing ends there.

    It yields each path as its directory's listing arrives, so the paths are not
    in order. fsspec's ``_find`` sorts them, by name. A storage listing a
    directory twice, e.g. as a prefix and as its marker object, yields it twice.

    It wins over fsspec's ``_find`` on storages that list one directory per
    request, with many directories: their listings overlap. It gains nothing on
    object stores, which list a whole tree in one paginated request: it sends
    that same listing.

    Once cancelled, it lists no new directory, and yields the paths of the
    listings in flight.

    Requests: one recursive ``_ils`` of ``path``, which a storage without a
    recursive listing answers with one per directory. With ``maxdepth``, one
    ``_ils`` per directory instead, as the filesystem's ``operation_limit``
    allows. One ``info`` more for ``path`` itself, if a file, or a directory
    with ``withdirs``.

    ``path`` is the directory searched on ``fs``, by its ``_ils`` and ``_info``.
    ``maxdepth`` of 1 finds only the files in ``path``. ``withdirs`` finds
    directories too, ``path`` included. ``ils_kwargs`` go to each ``_ils``, as
    fsspec's ``find`` passes its own on.

    The operation yields the ``_ils`` entry of each path found, or the error
    of a directory that failed to list, and its state is a `FindState`. The call
    raises ValueError if ``maxdepth`` is less than 1.
    """
    _check_maxdepth(maxdepth)

    async def run(
        op: Operation[dict[str, Any], FindState],
    ) -> AsyncGenerator[dict[str, Any] | Exception]:
        root = _trim(path)
        steps = _find_listings(
            fs,
            root,
            maxdepth=maxdepth,
            ils_kwargs=ils_kwargs,
            stopping=op.cancelling,
            file_root=True,
            dir_root=withdirs and bool(root),
        )
        state = op.state

        async with aclosing(steps):
            async for listing in steps:
                found = [
                    e for e in listing.entries if withdirs or e["type"] != "directory"
                ]
                op.state = state = FindState(
                    errors=state.errors + (listing.error is not None),
                    current=listing.directory,
                    # Depth 0 is the root's own entry: no directory listed.
                    directories_listed=state.directories_listed
                    + (listing.depth > 0 and listing.last),
                    found=state.found + len(found),
                )

                if listing.error is not None:
                    yield listing.error

                for entry in found:
                    yield entry

    return Operation(run, FindState(directories_listed=0))


async def _find_listings(
    fs: Any,
    path: str,
    *,
    maxdepth: int | None = None,
    descend: Callable[[dict[str, Any]], bool] | None = None,
    whole: Callable[[dict[str, Any]], bool] | None = None,
    ils_kwargs: dict[str, Any] | None = None,
    stopping: Callable[[], object] | None = None,
    file_root: bool = False,
    dir_root: bool = False,
) -> AsyncGenerator[_Listing]:
    """Walk as the strategies that only read do, such as find, du and glob.

    It yields each directory's listing in parts, as ``_chunks`` does, but
    treats ``path`` itself as fsspec's ``_find`` and ``_glob`` do:

    - missing, it yields nothing;
    - a file, it yields the file's ``info`` entry if ``file_root``, else
      nothing;
    - a directory, it yields the directory's own ``info`` entry after its
      listing's last part, if ``dir_root`` and the walk is not stopping.

    The root's own entry comes as a `_Listing` of depth 0, which is no listing.
    So the strategies filter and count it as any entry, but count no directory
    listed for it.
    """
    root = _trim(path)
    listings = _chunks(
        fs,
        root,
        maxdepth=maxdepth,
        descend=descend,
        whole=whole,
        ils_kwargs=ils_kwargs,
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
                async with fs.operation_limit:
                    entry = await fs._info(root)
                yield _Listing(root, 0, [entry])
            return

        yield root_listing
        if root_listing.last:
            async for listing in _root_entry(fs, root, dir_root, stopping):
                yield listing

        async for listing in listings:
            yield listing
            if listing.directory == root and listing.last:
                async for entry in _root_entry(fs, root, dir_root, stopping):
                    yield entry


async def _root_entry(
    fs: Any,
    root: str,
    dir_root: bool,
    stopping: Callable[[], object] | None,
) -> AsyncGenerator[_Listing]:
    """The root directory's own ``info`` entry, as a listing of depth 0, if asked."""
    if dir_root and not (stopping and stopping()):
        async with fs.operation_limit:
            entry = await fs._info(root)
        yield _Listing(root, 0, [entry])
