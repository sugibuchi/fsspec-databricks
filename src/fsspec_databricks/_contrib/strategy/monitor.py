# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A filesystem whose calls the monitors see: as it is, or wrapped.

`monitored` returns a filesystem that tells the monitors of a `monitoring`
block of its operations. One inheriting the interface's `Monitored`, such as a
`TreeFileSystem` or a wrapper, already does. Any other is wrapped as its entry
in the known implementations says: none is chosen for a filesystem not there.

This module depends on `wrapper`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem

from fsspec_alt_strategies.monitor import Monitored
from fsspec_alt_strategies.profile import StorageProfile
from fsspec_alt_strategies.wrapper import known_implementations

from .wrapper import (
    AsyncFlatWrapper,
    AsyncTreeWrapper,
    SyncFlatWrapper,
    SyncTreeWrapper,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.wrapper import KnownImplementation

__all__ = ["monitored"]


def _wrapper(kind: str, asynchronous: bool) -> Any:
    """The wrapper class of a family, async or not."""
    if kind == "tree":
        return AsyncTreeWrapper if asynchronous else SyncTreeWrapper
    return AsyncFlatWrapper if asynchronous else SyncFlatWrapper


def monitored(
    fs: AbstractFileSystem,
    implementations: Mapping[str, KnownImplementation] | None = None,
    wrap_unknown_fs: bool = False,
) -> AbstractFileSystem:
    """The filesystem, as one whose calls the monitors see.

    A filesystem inheriting `Monitored` is returned as it is. Any other is
    wrapped, as the entry of its class says, or of the nearest class it
    inherits: as a `TreeFileSystem` or a `FlatFileSystem`, as its profile's
    ``fs_type`` says, a tree if unknown, async or not, with the entry's limits
    and profile. The wrapper is new, and may be dropped
    once used.

    A filesystem no entry names is wrapped only if ``wrap_unknown_fs``: as
    a `TreeFileSystem`, one request at a time, with an unknown profile. That
    is right on any storage, if slow. It is async as the filesystem is an
    `AsyncFileSystem`.

    Parameters
    ----------
    fs : AbstractFileSystem
        The filesystem.
    implementations : mapping, optional
        How each filesystem class is wrapped, by the full name of the class,
        e.g. "s3fs.core.S3FileSystem". The interface's
        `known_implementations` if None.
    wrap_unknown_fs : bool, default False
        Whether to wrap a filesystem no entry names, the way right on any
        storage, rather than raise.

    Returns
    -------
    AbstractFileSystem
        ``fs``, or a wrapper of it.

    Raises
    ------
    ValueError
        If ``fs`` is not monitored, no entry names its class, and
        ``wrap_unknown_fs`` is False.

    Examples
    --------
    >>> with monitoring(Progress()):
    ...     monitored(fsspec.filesystem("file")).rm("data/2024", recursive=True)
    """
    if isinstance(fs, Monitored):
        return fs

    table = known_implementations if implementations is None else implementations
    for cls in type(fs).__mro__:
        entry = table.get(f"{cls.__module__}.{cls.__qualname__}")
        if entry is not None:
            break
    else:
        if not wrap_unknown_fs:
            name = f"{type(fs).__module__}.{type(fs).__qualname__}"
            raise ValueError(f"No known implementation says how to wrap {name}")
        entry = {
            "storage_profile": StorageProfile(fs_type="tree"),
            "async": isinstance(fs, AsyncFileSystem),
            "operation_concurrency": 1,
            "io_concurrency": 1,
        }

    # A family unknown is a tree, the way right on any storage.
    fs_type = entry["storage_profile"].fs_type or "tree"
    wrapper = _wrapper(fs_type, entry["async"])
    options: dict[str, Any] = {
        "operation_concurrency": entry["operation_concurrency"],
        "io_concurrency": entry["io_concurrency"],
        "storage_profile": entry["storage_profile"],
    }
    if fs_type == "flat":
        options["rm_batch_size"] = entry["rm_batch_size"]
    return wrapper(fs, **options)
