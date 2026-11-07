# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Paths and depths, as the strategies take them, and folders as fsspec's walk gives them.

This module depends on nothing else in the package.
"""

from __future__ import annotations

from typing import Any


def _trim(path: str) -> str:
    """A path without its trailing "/", as fsspec's ``_strip_protocol`` leaves it.

    A storage's root stays as it is, e.g. "/" on a local filesystem.
    """
    return path.rstrip("/") or path


def _check_maxdepth(maxdepth: int | None) -> None:
    """Raise ValueError for a ``maxdepth`` below 1, as fsspec's walk does."""
    if maxdepth is not None and maxdepth < 1:
        raise ValueError(f"maxdepth must be at least 1: {maxdepth}")


def _triples(
    folder: str, entries: list[dict[str, Any]], detail: bool
) -> tuple[str, Any, Any]:
    """A folder's ``(path, dirs, files)``, as fsspec's ``walk`` yields them."""
    dirs: dict[str, Any] = {}
    files: dict[str, Any] = {}
    for entry in entries:
        name = entry["name"].rstrip("/").rsplit("/", 1)[-1]
        (dirs if entry["type"] == "directory" else files)[name] = entry
    if detail:
        return folder, dirs, files
    return folder, list(dirs), list(files)
