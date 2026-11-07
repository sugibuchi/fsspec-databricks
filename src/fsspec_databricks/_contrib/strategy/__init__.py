# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Alternative, filesystem-independent strategies for fsspec.

fsspec's defaults assume a flat key space, as object stores have. The strategies
here are written for filesystems with a hierarchical namespace too, and work on
any fsspec filesystem, as functions or as bases of a filesystem class.

They are meant to be copied into a project, not installed. Each is imported
from its module, so that a copy may leave out those it does not use. The
interface they share, such as `OperationHandler` and `Monitor`, is not copied:
it is the package ``fsspec_alt_strategies_common``, installed once, so that all
copies in a project share it.

`monitored`, which wraps a filesystem so that the monitors see its calls, is
re-exported here. It needs the wrappers of both families: a copy keeping it
keeps `tree` and `flat`.
"""

from __future__ import annotations

from .monitor import monitored

try:
    from ._version import __version__ as __version__
except ImportError:  # A source tree never built, or a copy of one: no version file.
    __version__ = "0.0.0"

__all__ = ["monitored"]
