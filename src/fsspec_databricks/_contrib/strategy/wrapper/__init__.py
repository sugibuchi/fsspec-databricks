# SPDX-FileCopyrightText: 2026 Tsuyoshi SUGIBUCHI
# SPDX-License-Identifier: Apache-2.0
"""Existing filesystems, wrapped so that their recursive methods are the strategies.

- `tree`: `AsyncTreeWrapper` and `SyncTreeWrapper`, as a `TreeFileSystem`;
- `flat`: `AsyncFlatWrapper` and `SyncFlatWrapper`, as a `FlatFileSystem`;
- `s3fs`, `gcsfs` and `adlfs`: the wrappers of those filesystems, using
  their own listings and copies where the generic ones would cost more.

A wrapper is light, and not cached, as fsspec caches filesystems: make one on
demand, e.g. to watch a call with a monitor, and drop it after. The developer
chooses the wrapper, or declares it in the known implementations: none is
detected.

Each class is imported from its module. This package imports none of them.
"""
