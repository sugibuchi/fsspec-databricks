# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Strategies that walk a folder tree, a folder per listing request.

They suit storages with a hierarchical namespace, which list one folder per
request. Each module holds one strategy: import it from there, e.g.
``fsspec_alt_strategies_impl.tree.find``. This package imports none of them, so a
copy may leave out those it does not use.
"""
