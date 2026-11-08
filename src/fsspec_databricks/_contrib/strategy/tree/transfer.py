# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A recursive copy from one storage to another, copying while it walks.

`tree_transfer` is `tree_cp` between two storages. It walks the source tree,
and copies each folder's files to the destination as soon as the folder is
listed, each by a ``copy_file`` given by the caller. `common.routing` has one
streaming the data through the two storages' file objects.

This module depends on `cp`, `_jobs`, and `operation` and `placement` of
`common`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fsspec_alt_strategies_common.states import CpState

from ..common.operation import Operation
from .cp import _Transfer

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Sequence

    from fsspec_alt_strategies_common.profile import StorageProfile

    from ..common.types import _Info, _Ls, _Request

__all__ = ["CpState", "tree_transfer"]


def tree_transfer(
    path1: str | Sequence[str],
    path2: str,
    *,
    ls: _Ls,
    info: _Info,
    copy_file: _Request,
    dst_info: _Info,
    dst_makedirs: _Request,
    dst_rm_file: _Request,
    profile: StorageProfile,
    concurrency_limit: asyncio.Semaphore,
) -> Operation[tuple[str, str], CpState]:
    """Copy a folder and everything in it to another storage, while walking it.

    It places the paths as `tree_cp` does, as fsspec's ``copy`` with
    ``recursive=True`` places them: into ``path2`` if it is an existing
    folder of the destination, or ends with "/", and onto it otherwise.
    Glob patterns and lists of paths are placed as `tree_cp` places them,
    and two paths of a list with the same name are reported as it does.
    Existing files are overwritten.

    The two storages are apart, so a path may be copied to the same path, or
    under itself: neither is refused, unlike in `tree_cp`.

    The source is walked as `tree_walk` walks it, with ``ls`` and ``info``.
    Each folder's files are copied as soon as the folder is listed, each by a
    ``copy_file``. A folder is created at the destination before files are
    copied into it, and for each empty folder.

    It assumes that ``copy_file`` copies a whole file, and that the
    destination behaves as ``profile`` says. The copies run concurrently, so
    the destination's paths are created in another order than the source's.

    It wins over fsspec's way, a ``get`` to local files then a ``put``, or a
    ``copy`` listing the whole tree first. The copies start after the first
    listing, and the data needs no local disk. Within one storage copying on
    its server, it loses to `tree_cp`: here every byte goes through the
    caller.

    Unlike fsspec's ``copy``, it keeps going past a failure, and reports each.
    A copy cancelled or failed leaves what was copied so far: it deletes
    nothing.

    Once cancelled, it starts no new listing or copy, and yields the outcomes
    of the copies already sent. Closed, it waits for those copies without
    yielding them. Iterating it raises FileNotFoundError if ``path1`` does not
    exist, or matches nothing.

    Requests: one ``dst_info`` for ``path2``, unless it ends with "/" or
    ``path1`` is a pattern or a list. Patterns, or a list, add the requests
    of their `tree_glob` on the source. One ``ls`` per source folder, one
    ``copy_file`` per file, and one ``dst_makedirs`` per destination folder
    with files or empty. A destination refusing to overwrite costs a
    ``dst_info``, a ``dst_rm_file`` and a ``copy_file`` more per file it
    refused. Each request holds a permit of ``concurrency_limit`` while it
    runs, a ``copy_file`` too, though it uses both storages.

    The profile's ``parents_required=False``, or ``implicit_folders=True``,
    saves the ``dst_makedirs`` of each folder with files, as their copies
    create it.

    Parameters
    ----------
    path1 : str or sequence of str
        The source's folder or file to copy, a glob pattern, or a list of
        them, as ``ls`` takes paths.
    path2 : str
        Where to copy it in the destination, as ``dst_info`` takes paths.
    ls : callable
        Lists a source folder, called as ``ls(path, detail=True)``.
    info : callable
        Describes a source path, called as ``info(path)``.
    copy_file : callable
        Copies a file from the source to the destination, called as
        ``copy_file(source, destination)``: e.g. one made by
        ``common.routing.streaming_copy`` from both ``open_async``.
    dst_info : callable
        Describes a destination path, called as ``dst_info(path)``.
    dst_makedirs : callable
        Creates a destination folder and its parents, called as
        ``dst_makedirs(path, exist_ok=True)``.
    dst_rm_file : callable
        Deletes a destination file, called as ``dst_rm_file(path)``. Only a
        storage refusing to overwrite needs it.
    profile : StorageProfile
        What the destination does: ``StorageProfile()`` if it is unknown.
    concurrency_limit : asyncio.Semaphore
        Held by each request while it runs, on either storage.

    Returns
    -------
    Operation
        Yielding ``(source, destination)`` for each file copied, or an error.
        Its state is a `CpState`.

    See Also
    --------
    tree_cp : The same, within one storage.

    Examples
    --------
    >>> transfer = tree_transfer(
    ...     "data/2024",
    ...     "backup/",
    ...     ls=src._ls,
    ...     info=src._info,
    ...     copy_file=streaming_copy(src.open_async, dst.open_async),
    ...     dst_info=dst._info,
    ...     dst_makedirs=dst._makedirs,
    ...     dst_rm_file=dst._rm_file,
    ...     profile=StorageProfile(),
    ...     concurrency_limit=asyncio.Semaphore(32),
    ... )
    >>> async with transfer as op:
    ...     async for copied in op:
    ...         if isinstance(copied, Exception):
    ...             print("failed:", copied)
    >>> op.state.files_copied
    1234
    """
    return Operation(
        lambda op: _CrossCopy(
            path1,
            path2,
            ls=ls,
            info=info,
            copy_file=copy_file,
            destination_info=dst_info,
            makedirs=dst_makedirs,
            dst_rm_file=dst_rm_file,
            profile=profile,
            concurrency_limit=concurrency_limit,
            op=op,
        ).outcomes(),
        CpState(),
    )


class _CrossCopy(_Transfer):
    """The jobs of one `tree_transfer`: a ``copy_file`` per file."""

    def __init__(
        self,
        path1: str | Sequence[str],
        path2: str,
        *,
        copy_file: _Request,
        dst_rm_file: _Request,
        **kwargs: Any,
    ) -> None:
        super().__init__(path1, path2, **kwargs)
        self.copy_file = copy_file
        self.dst_rm_file = dst_rm_file

    async def transfer_file(self, source: str, destination: str) -> None:
        try:
            try:
                await self.copy_file(source, destination)
            except FileExistsError:
                # The destination refuses to overwrite, as tree_cp handles it.
                # The job holds its permit already.
                entry = await self.destination_info(destination)
                if entry["type"] == "directory":
                    raise

                await self.dst_rm_file(destination)
                await self.copy_file(source, destination)
        except Exception as e:
            # Yielded among paths, the error must tell its paths itself.
            e.add_note(f"{self.doing} {source!r} to {destination!r} failed.")
            raise
