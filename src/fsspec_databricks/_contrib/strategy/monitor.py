# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""A filesystem whose calls the monitors see: as it is, or wrapped.

`monitored` returns a filesystem that tells the monitors of a `monitoring`
block of its operations. One inheriting the interface's `Monitored`, such as a
`TreeFileSystem` or a wrapper, already does. Any other is wrapped as its entry
in the known implementations says: none is chosen for a filesystem not there.

This module depends on `adlfs`, `flat`, `gcsfs`, `s3fs` and `tree` of
`wrapper`, and `filesystem` of `tree` and of `flat`.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from fsspec.asyn import AsyncFileSystem

from fsspec_alt_strategies.monitor import Monitored
from fsspec_alt_strategies.profile import StorageProfile
from fsspec_alt_strategies.wrapper import known_implementations

from .flat.filesystem import FlatFileSystem
from .tree.filesystem import TreeFileSystem
from .wrapper.adlfs import AdlfsWrapper
from .wrapper.flat import AsyncFlatWrapper, SyncFlatWrapper
from .wrapper.gcsfs import GcsfsWrapper
from .wrapper.s3fs import S3fsWrapper
from .wrapper.tree import AsyncTreeWrapper, SyncTreeWrapper

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fsspec.spec import AbstractFileSystem

    from fsspec_alt_strategies.wrapper import KnownImplementation

__all__ = ["monitored"]

_BUILT_IN: dict[str, type] = {
    cls.__name__: cls
    for cls in (
        AsyncTreeWrapper,
        SyncTreeWrapper,
        AsyncFlatWrapper,
        SyncFlatWrapper,
        S3fsWrapper,
        GcsfsWrapper,
        AdlfsWrapper,
    )
}
"""The wrappers an entry names by their bare class name: every copy has them."""


def _named(name: str) -> type:
    """The wrapper class an entry names: built in by a bare name, else imported by its full name."""
    module, _, qualname = name.rpartition(".")
    if not module:
        if name not in _BUILT_IN:
            raise ValueError(f"No built-in wrapper is named {name}")
        return _BUILT_IN[name]
    else:
        cls = getattr(importlib.import_module(module), qualname, None)
        if not isinstance(cls, type):
            raise ValueError(f"{module} has no class {qualname}")
        return cls


def _wrapper(entry: KnownImplementation, kind: str, asynchronous: bool) -> type:
    """The wrapper class of an entry: the one it names, or its family's, async or not."""
    if "wrapper" not in entry:
        if kind == "tree":
            return AsyncTreeWrapper if asynchronous else SyncTreeWrapper
        else:
            return AsyncFlatWrapper if asynchronous else SyncFlatWrapper

    cls = _named(entry["wrapper"])
    family = TreeFileSystem if kind == "tree" else FlatFileSystem
    if not issubclass(cls, family):
        # A wrapper of another family would answer as a profile it does not have.
        raise ValueError(f"{entry['wrapper']} is not a {family.__name__}")
    return cls


def monitored(
    fs: AbstractFileSystem,
    implementations: Mapping[str, KnownImplementation] | None = None,
    wrap_unknown_fs: bool = False,
) -> AbstractFileSystem:
    """The filesystem, as one whose calls the monitors see.

    A filesystem inheriting `Monitored` is returned as it is. Any other is
    wrapped, as the entry of its class says, or of the nearest class it
    inherits. Its profile's ``fs_type`` picks a `TreeFileSystem` or a
    `FlatFileSystem`, a tree if unknown, and the entry gives its limits. The
    wrapper is the family's own, async if the filesystem is an
    `AsyncFileSystem`. It is new, and may be dropped once used.

    An entry's ``wrapper`` names another class, of the same family:

    - a bare class name is a wrapper built in, e.g. "S3fsWrapper", as the
      known entries of s3fs, gcsfs and adlfs name theirs;
    - a full name is a class of the caller's own, e.g.
      "mypkg.wrappers.MyWrapper", imported here.

    A filesystem no entry names is wrapped only if ``wrap_unknown_fs``: as
    a `TreeFileSystem`, one request at a time, with an unknown profile. That
    is right on any storage, if slow.

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
        ``wrap_unknown_fs`` is False. If the entry's ``wrapper`` names no
        class, or one of another family.
    ImportError
        If the module of the entry's ``wrapper`` cannot be imported.

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
            "operation_concurrency": 1,
            "io_concurrency": 1,
        }

    # A family unknown is a tree, the way right on any storage.
    fs_type = entry["storage_profile"].fs_type or "tree"
    wrapper: Any = _wrapper(entry, fs_type, isinstance(fs, AsyncFileSystem))
    options: dict[str, Any] = {
        "operation_concurrency": entry["operation_concurrency"],
        "io_concurrency": entry["io_concurrency"],
        "storage_profile": entry["storage_profile"],
    }
    if fs_type == "flat":
        options["rm_batch_size"] = entry["rm_batch_size"]
    return wrapper(fs, **options)
