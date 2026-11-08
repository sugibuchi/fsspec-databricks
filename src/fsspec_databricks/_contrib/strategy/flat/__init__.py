# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Strategies that list a flat namespace by key prefix, as object stores do.

They suit storages without real folders, which list every key under a prefix
in one paginated request. They take that listing as ``iexpand``: a function
giving an async iterator of the entries, in order of their names. Each module
holds one strategy: import it from there, e.g.
``fsspec_alt_strategies_impl.flat.find``. This package imports none of them, so
a copy may leave out those it does not use.
"""
